import collections
from datetime import datetime
from pytz import timezone

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass
from backtrader import Order
from backtrader.position import Position

from QuikPy import QuikPy


class MetaSingleton(MetaParams):
    """Метакласс для создания Singleton классов"""
    def __init__(cls, *args, **kwargs):
        """Инициализация класса"""
        super(MetaSingleton, cls).__init__(*args, **kwargs)
        cls._singleton = None  # Экземпляра класса еще нет

    def __call__(cls, *args, **kwargs):
        """Вызов класса"""
        if not cls._singleton:  # Если класса нет в экземплярах класса
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)  # то создаем зкземпляр класса
        return cls._singleton  # Возвращаем экземпляр класса


class QKStore(with_metaclass(MetaSingleton, object)):
    """Хранилище QUIK"""
    params = (
        ('Host', '127.0.0.1'),  # Адрес/IP компьютера с QUIK
        ('RequestsPort', 34130),  # Номер порта для запросов и ответов
        ('CallbacksPort', 34131),  # Номер порта для получения событий
        ('StopSteps', 10),  # Размер в минимальных шагах цены инструмента для исполнения стоп заявок
    )

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    MarketTimeZone = timezone('Europe/Moscow')  # Биржа работает по московскому времени

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns DataCls with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered BrokerCls"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(QKStore, self).__init__()
        self.notifs = collections.deque()  # Уведомления хранилища
        self.isConnected = True  # Считаем, что изначально QUIK подключен к серверу брокера
        self.qpProvider = QuikPy(Host=self.p.Host, RequestsPort=self.p.RequestsPort, CallbacksPort=self.p.CallbacksPort)  # Вызываем конструктор QuikPy с адресом хоста и портами
        self.classCodes = self.qpProvider.GetClassesList()['data']  # Список классов. В некоторых таблицах тикер указывается без кода класса
        self.subscribedSymbols = []  # Список подписанных тикеров/интервалов
        self.symbols = {}  # Информация о тикерах
        self.newBars = []  # Новые бары по подписке из QUIK
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.orderNums = {}  # Словарь заявок на бирже. Индекс - номер транзакции, значение - номер заявки на бирже
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)
        self.ocos = {}  # Список связанных заявок (One Cancel Others)

    def start(self):
        self.qpProvider.OnNewCandle = self.on_new_bar  # Обработчик новых баров по подписке из QUIK

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [notif for notif in iter(self.notifs.popleft, None)]

    def stop(self):
        self.qpProvider.OnNewCandle = self.qpProvider.DefaultHandler  # Возвращаем обработчик по умолчанию
        self.qpProvider.CloseConnectionAndThread()  # Закрываем соединение для запросов и поток обработки функций обратного вызова

    # Функции

    def get_symbol_info(self, class_code, sec_code, reload=False):
        """Получение информации тикера

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param bool reload: Получить информацию из QUIK
        :return: Значение из кэша/QUIK или None, если тикер не найден
        """
        if reload or (class_code, sec_code) not in self.symbols:  # Если нужно получить информацию из QUIK или нет информации о тикере в справочнике
            symbol_info = self.qpProvider.GetSecurityInfo(class_code, sec_code)  # Получаем информацию о тикере из QUIK
            if 'data' not in symbol_info:  # Если ответ не пришел (возникла ошибка). Например, для опциона
                print(f'Информация о {self.class_sec_code_to_data_name(class_code, sec_code)} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(class_code, sec_code)] = symbol_info['data']  # Заносим информацию о тикере в справочник
        return self.symbols[(class_code, sec_code)]  # Возвращаем значение из справочника

    def data_name_to_class_sec_code(self, dataname):
        """Код площадки и код тикера из названия тикера (с кодом площадки или без него)

        :param str dataname: Название тикера
        :return: Код площадки и код тикера
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код площадки>.<Код тикера>
            class_code = symbol_parts[0]  # Код площадки
            sec_code = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без площадки
            class_code = self.qpProvider.GetSecurityClass(self.classCodes, dataname)['data']  # Получаем код площадки по коду инструмента из имеющихся классов
            sec_code = dataname  # Код тикера
        return class_code, sec_code  # Возвращаем код площадки и код тикера

    @staticmethod
    def class_sec_code_to_data_name(class_code, sec_code):
        """Название тикера из кода площадки и кода тикера

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :return: Название тикера
        """
        return f'{class_code}.{sec_code}'

    def size_to_lots(self, class_code, sec_code, size: int):
        """Перевод кол-ва из штук в лоты

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param int size: Кол-во в штуках
        :return: Кол-во в лотах
        """
        si = self.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (lot_size)
        if not si:  # Если тикер не найден
            return size  # то кол-во не изменяется
        lot_size = int(si['lot_size'])  # Размер лота тикера
        return int(size / lot_size) if lot_size > 0 else size  # Если задан лот, то переводим

    def lots_to_size(self, class_code, sec_code, lots: int):
        """Перевод кол-ва из лотов в штуки

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param int lots: Кол-во в лотах
        :return: Кол-во в штуках
        """
        si = self.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (lot_size)
        if not si:  # Если тикер не найден
            return lots  # то лот не изменяется
        lot_size = int(si['lot_size'])  # Размер лота тикера
        return lots * lot_size if lot_size > 0 else lots  # Если задан лот, то переводим

    def bt_to_quik_price(self, class_code, sec_code, price: float):
        """Перевод цен из BackTrader в QUIK

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param float price: Цена в BackTrader
        :return: Цена в QUIK
        """
        if class_code == 'TQOB':  # Для рынка облигаций
            return price / 10  # цену делим на 10
        if class_code == 'SPBFUT':  # Для рынка фьючерсов
            si = self.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (lot_size)
            if not si:  # Если тикер не найден
                return price  # то цена не изменяется
            lot_size = int(si['lot_size'])  # Размер лота тикера
            if lot_size > 0:  # Если лот задан
                return price * lot_size  # то цену умножаем на лот
        return price  # В остальных случаях цена не изменяется

    def quik_to_bt_price(self, class_code, sec_code, price: float):
        """Перевод цен из QUIK в BackTrader

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param float price: Цена в QUIK
        :return: Цена в BackTrader
        """
        if class_code == 'TQOB':  # Для рынка облигаций
            return price * 10  # цену умножаем на 10
        if class_code == 'SPBFUT':  # Для рынка фьючерсов
            si = self.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (lot_size)
            if not si:  # Если тикер не найден
                return price  # то цена не изменяется
            lot_size = int(si['lot_size'])  # Размер лота тикера
            if lot_size > 0:  # Если лот задан
                return price / lot_size  # то цену делим на лот
        return price  # В остальных случаях цена не изменяется

    # QKBroker

    def get_positions(self, client_code, firm_id, limit_kind, is_lots, is_futures=False):
        """Все активные позиции по счету

        :param str client_code: Код клиента
        :param str firm_id: Код фирмы
        :param int limit_kind: День лимита
        :param bool is_lots: Входящий остаток в лотах
        :param bool is_futures: Фьючерсный счет
        """
        if is_futures:  # Для фьючерсов свои расчеты
            futures_holdings = self.qpProvider.GetFuturesHoldings()['data']  # Все фьючерсные позиции
            active_futures_holdings = [futures_holding for futures_holding in futures_holdings if futures_holding['totalnet'] != 0]  # Активные фьючерсные позиции
            for active_futures_holding in active_futures_holdings:  # Пробегаемся по всем активным фьючерсным позициям
                class_code = 'SPBFUT'  # Код площадки
                sec_code = active_futures_holding['sec_code']  # Код тикера
                dataname = self.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
                size = active_futures_holding['totalnet']  # Кол-во
                if is_lots:  # Если входящий остаток в лотах
                    size = self.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                price = float(active_futures_holding['avrposnprice'])  # Цена приобретения
                price = self.quik_to_bt_price(class_code, sec_code, price)  # Переводим цену приобретения за лот в цену приобретения за штуку
                self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций
        else:  # Для остальных фирм
            depo_limits = self.qpProvider.GetAllDepoLimits()['data']  # Все лимиты по бумагам (позиции по инструментам)
            account_depo_limits = [depo_limit for depo_limit in depo_limits  # Бумажный лимит
                                   if depo_limit['client_code'] == client_code and  # выбираем по коду клиента
                                   depo_limit['firmid'] == firm_id and  # фирме
                                   depo_limit['limit_kind'] == limit_kind and  # дню лимита
                                   depo_limit['currentbal'] != 0]  # только открытые позиции
            for firm_kind_depo_limit in account_depo_limits:  # Пробегаемся по всем позициям
                dataname = firm_kind_depo_limit['sec_code']  # В позициях код тикера указывается без кода площадки
                class_code, sec_code = self.data_name_to_class_sec_code(dataname)  # По коду тикера без площадки получаем код площадки и код тикера
                size = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                if is_lots:  # Если входящий остаток в лотах
                    size = self.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                price = float(firm_kind_depo_limit['wa_position_price'])  # Цена приобретения
                price = self.quik_to_bt_price(class_code, sec_code, price)  # Для рынка облигаций цену приобретения умножаем на 10
                dataname = self.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
                self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций

    def get_money_limits(self, client_code, firm_id, trade_account_id, limit_kind, currency_code, is_futures=False):
        """Свободные средства по счету

        :param str client_code: Код клиента
        :param str firm_id: Код фирмы
        :param str trade_account_id: Счет
        :param int limit_kind: День лимита
        :param str currency_code: Валюта
        :param bool is_futures: Фьючерсный счет
        :return: Свободные средства по счету или None
        """
        if is_futures:  # Для фьючерсов свои расчеты
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход включает Биржевые сборы
            # Тек.чист.поз. = Заблокированное ГО под открытые позиции
            # План.чист.поз. = На какую сумму можете открыть еще позиции
            try:
                futures_limit = self.qpProvider.GetFuturesLimit(firm_id, trade_account_id, 0, 'SUR')['data']  # Фьючерсные лимиты
                return float(futures_limit['cbplimit']) + float(futures_limit['varmargin']) + float(futures_limit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                print(f'QUIK не вернул фьючерсные лимиты с FirmId={firm_id}, TradeAccountId={trade_account_id}. Проверьте правильность значений')
                return None
        # Для остальных фирм
        money_limits = self.qpProvider.GetMoneyLimits()['data']  # Все денежные лимиты (остатки на счетах)
        if len(money_limits) == 0:  # Если денежных лимитов нет
            print('QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return None
        cash = [money_limit for money_limit in money_limits  # Из всех денежных лимитов
                if money_limit['client_code'] == client_code and  # выбираем по коду клиента
                money_limit['firmid'] == firm_id and  # фирме
                money_limit['limit_kind'] == limit_kind and  # дню лимита
                money_limit["currcode"] == currency_code]  # и валюте
        if len(cash) != 1:  # Если ни один денежный лимит не подходит
            print(f'Денежный лимит не найден с ClientCode={client_code}, FirmId={firm_id}, LimitKind={limit_kind}, CurrencyCode={currency_code}. Проверьте правильность значений')
            # print(f'Полученные денежные лимиты: {money_limits}')  # Для отладки, если нужно разобраться, что указано неверно
            return None
        return float(cash[0]['currentbal'])  # Денежный лимит (остаток) по счету

    def get_positions_limits(self, firm_id, trade_account_id, is_futures=False):
        """
        Стоимость позиций по счету

        :param str firm_id: Код фирмы
        :param str trade_account_id: Счет
        :param bool is_futures: Фьючерсный счет
        :return: Стоимость позиций по счету или None
        """
        if is_futures:  # Для фьючерсов свои расчеты
            try:
                return float(self.qpProvider.GetFuturesLimit(firm_id, trade_account_id, 0, 'SUR')['data']['cbplused'])  # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                return None
        # Для остальных фирм
        pos_value = 0  # Стоимость позиций по счету
        for dataname in list(self.positions.keys()):  # Пробегаемся по копии позиций (чтобы не было ошибки при изменении позиций)
            class_code, sec_code = self.data_name_to_class_sec_code(dataname)  # По названию тикера получаем код площадки и код тикера
            last_price = float(self.qpProvider.GetParamEx(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
            last_price = self.quik_to_bt_price(class_code, sec_code, last_price)  # Для рынка облигаций последнюю цену сделки умножаем на 10
            pos = self.positions[dataname]  # Получаем позицию по тикеру
            pos_value += pos.size * last_price  # Добавляем стоимость позиции
        return pos_value  # Стоимость позиций по счету

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if not self.orders.get(order.ref, False):  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        if order.ref not in self.orderNums:  # Если заявки нет в словаре заявок на бирже
            return  # то выходим, дальше не продолжаем
        order_num = self.orderNums[order.ref]  # Номер заявки на бирже
        class_code, sec_code = self.data_name_to_class_sec_code(order.data._name)  # По названию тикера получаем код площадки и код тикера
        is_stop = order.exectype in [Order.Stop, Order.StopLimit] and \
            isinstance(self.qpProvider.GetOrderByNumber(order_num)['data'], int)  # Задана стоп заявка и лимитная заявка не выставлена
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': class_code,  # Код площадки
            'SECCODE': sec_code}  # Код тикера
        if is_stop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'  # Будем удалять стоп заявку
            transaction['STOP_ORDER_KEY'] = str(order_num)  # Номер стоп заявки на бирже
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'  # Будем удалять лимитную заявку
            transaction['ORDER_KEY'] = str(order_num)  # Номер заявки на бирже
        self.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на биржу
        return order  # В список уведомлений ничего не добавляем. Ждем события OnTransReply

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        for order_ref, oco_ref in self.ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = self.ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def on_connected(self, data):
        """Обработка событий подключения к QUIK"""
        dt = datetime.now(self.MarketTimeZone)  # Берем текущее время на бирже из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Connected')
        self.isConnected = True  # QUIK подключен к серверу брокера
        print(f'Проверка подписки тикеров ({len(self.subscribedSymbols)})')
        for subscribed_symbol in self.subscribedSymbols:  # Пробегаемся по всем подписанным тикерам
            class_code = subscribed_symbol['class']  # Код площадки
            sec_code = subscribed_symbol['sec']  # Код тикера
            interval = subscribed_symbol['interval']  # Временной интервал
            print(f'{self.class_sec_code_to_data_name(class_code, sec_code)} на интервале {interval}', end=' ')
            if not self.qpProvider.IsSubscribed(class_code, sec_code, interval)['data']:  # Если нет подписки на тикер/интервал
                self.qpProvider.SubscribeToCandles(class_code, sec_code, interval)  # то переподписываемся
                print('нет подписки. Отправлен запрос на новую подписку')
            else:  # Если подписка была, то переподписываться не нужно
                print('есть подписка')

    def on_disconnected(self, data):
        """Обработка событий отключения от QUIK"""
        if not self.isConnected:  # Если QUIK отключен от сервера брокера
            return  # то не нужно дублировать сообщение, выходим, дальше не продолжаем
        dt = datetime.now(self.MarketTimeZone)  # Берем текущее время на бирже из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Disconnected')
        self.isConnected = False  # QUIK отключен от сервера брокера

    # QKData

    def on_new_bar(self, data):
        """Обработка событий получения новых баров"""
        self.newBars.append(data['data'])  # Добавляем новый бар в список новых баров
