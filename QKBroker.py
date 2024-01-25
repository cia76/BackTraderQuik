import collections
from datetime import datetime, date
import time
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderQuik import QKStore


class MetaQKBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaQKBroker, cls).__init__(name, bases, dct)  # Инициализируем класс брокера
        QKStore.BrokerCls = cls  # Регистрируем класс брокера в хранилище QUIK


class QKBroker(with_metaclass(MetaQKBroker, BrokerBase)):
    """Брокер QUIK"""
    # TODO Сделать обертку для поддержки множества брокеров
    # TODO Сделать пример постановки заявок по разным портфелям
    # Обсуждение решения: https://community.backtrader.com/topic/1165/does-backtrader-support-multiple-brokers
    # Пример решения: https://github.com/JacobHanouna/backtrader/blob/ccxt_multi_broker/backtrader/brokers/ccxtmultibroker.py
    logger = logging.getLogger('QKBroker')

    params = (
        ('use_positions', True),  # При запуске брокера подтягиваются текущие позиции с биржи
        ('Lots', True),  # Входящий остаток в лотах (задается брокером)
        ('ClientCode', ''),  # Код клиента
        # По статье https://zen.yandex.ru/media/id/5e9a612424270736479fad54/bitva-s-finam-624f12acc3c38f063178ca95
        ('ClientCodeForOrders', ''),  # Номер торгового терминала. У брокера Финам требуется для совершения торговых операций
        ('FirmId', 'SPBFUT'),  # Фирма
        ('TradeAccountId', 'SPBFUT00PST'),  # Счет
        ('LimitKind', 0),  # День лимита
        ('CurrencyCode', 'SUR'),  # Валюта
        ('IsFutures', True),  # Фьючерсный счет
    )

    def __init__(self, **kwargs):
        super(QKBroker, self).__init__()
        self.store = QKStore(**kwargs)  # Хранилище QUIK
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = 0  # Стартовый и текущий баланс счета
        if not self.p.ClientCodeForOrders:  # Для брокера Финам нужно вместо кода клиента
            self.p.ClientCodeForOrders = self.p.ClientCode  # указать Номер торгового терминала
        self.trade_nums = dict()  # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

    def start(self):
        super(QKBroker, self).start()
        self.store.provider.OnTransReply = self.on_trans_reply  # Ответ на транзакцию пользователя
        self.store.provider.OnTrade = self.on_trade  # Получение новой / изменение существующей сделки
        if self.p.use_positions:  # Если нужно при запуске брокера получить текущие позиции на бирже
            self.get_all_active_positions(self.p.ClientCode, self.p.FirmId, self.p.LimitKind, self.p.Lots, self.p.IsFutures)  # То получаем их
        self.startingcash = self.cash = self.getcash()  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = self.getvalue()  # Стартовый и текущий баланс счета

    def getcash(self):
        """Свободные средства по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            cash = self.get_money_limits(self.p.ClientCode, self.p.FirmId, self.p.TradeAccountId, self.p.LimitKind, self.p.CurrencyCode, self.p.IsFutures)  # Свободные средства по счету
            if cash:  # Если свободные средства были получены
                self.cash = cash  # то запоминаем их
        return self.cash

    def getvalue(self, datas=None):
        """Стоимость позиций по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        # TODO Выдавать баланс по тикерам (datas) как в Alor
        # TODO Выдавать весь баланс, если не указан параметры. Иначе, выдавать баланс по параметрам
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            v = self.get_positions_limits(self.p.FirmId, self.p.TradeAccountId, self.p.IsFutures)  # Стоимость позиций по счету
            if v:  # Если стоимость позиций была получена
                self.value = v  # Баланс счета = свободные средства + стоимость позиций
        return self.value

    def getposition(self, data):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[data._name]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, ClientCode=self.p.ClientCodeForOrders, TradeAccountId=self.p.TradeAccountId, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на покупку на биржу
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, ClientCode=self.p.ClientCodeForOrders, TradeAccountId=self.p.TradeAccountId, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на продажу на биржу
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        if not self.notifs:  # Если в списке уведомлений ничего нет
            return None  # то ничего и возвращаем, выходим, дальше не продолжаем
        return self.notifs.popleft()  # Удаляем и возвращаем крайний левый элемент списка уведомлений

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(QKBroker, self).stop()
        self.store.provider.OnConnected = self.store.provider.DefaultHandler  # Соединение терминала с сервером QUIK
        self.store.provider.OnDisconnected = self.store.provider.DefaultHandler  # Отключение терминала от сервера QUIK
        self.store.provider.OnTransReply = self.store.provider.DefaultHandler  # Ответ на транзакцию пользователя
        self.store.provider.OnTrade = self.store.provider.DefaultHandler  # Получение новой / изменение существующей сделки
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_all_active_positions(self, client_code, firm_id, limit_kind, is_lots, is_futures=False):
        """Все активные позиции по счету

        :param str client_code: Код клиента
        :param str firm_id: Код фирмы
        :param int limit_kind: День лимита
        :param bool is_lots: Входящий остаток в лотах
        :param bool is_futures: Фьючерсный счет
        """
        if is_futures:  # Для фьючерсов свои расчеты
            futures_holdings = self.store.provider.GetFuturesHoldings()['data']  # Все фьючерсные позиции
            active_futures_holdings = [futures_holding for futures_holding in futures_holdings if futures_holding['totalnet'] != 0]  # Активные фьючерсные позиции
            for active_futures_holding in active_futures_holdings:  # Пробегаемся по всем активным фьючерсным позициям
                class_code = 'SPBFUT'  # Код площадки
                sec_code = active_futures_holding['sec_code']  # Код тикера
                dataname = self.store.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
                size = active_futures_holding['totalnet']  # Кол-во
                if is_lots:  # Если входящий остаток в лотах
                    size = self.store.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                price = float(active_futures_holding['avrposnprice'])  # Цена приобретения
                price = self.store.quik_to_bt_price(class_code, sec_code, price)  # Переводим цену приобретения за лот в цену приобретения за штуку
                self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций
        else:  # Для остальных фирм
            depo_limits = self.store.provider.GetAllDepoLimits()['data']  # Все лимиты по бумагам (позиции по инструментам)
            account_depo_limits = [depo_limit for depo_limit in depo_limits  # Бумажный лимит
                                   if depo_limit['client_code'] == client_code and  # выбираем по коду клиента
                                   depo_limit['firmid'] == firm_id and  # фирме
                                   depo_limit['limit_kind'] == limit_kind and  # дню лимита
                                   depo_limit['currentbal'] != 0]  # только открытые позиции
            for firm_kind_depo_limit in account_depo_limits:  # Пробегаемся по всем позициям
                dataname = firm_kind_depo_limit['sec_code']  # В позициях код тикера указывается без кода площадки
                class_code, sec_code = self.store.data_name_to_class_sec_code(dataname)  # По коду тикера без площадки получаем код площадки и код тикера
                size = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                if is_lots:  # Если входящий остаток в лотах
                    size = self.store.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                price = float(firm_kind_depo_limit['wa_position_price'])  # Цена приобретения
                price = self.store.quik_to_bt_price(class_code, sec_code, price)  # Для рынка облигаций цену приобретения умножаем на 10
                dataname = self.store.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
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
                futures_limit = self.store.provider.GetFuturesLimit(firm_id, trade_account_id, 0, 'SUR')['data']  # Фьючерсные лимиты
                return float(futures_limit['cbplimit']) + float(futures_limit['varmargin']) + float(futures_limit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                print(f'QUIK не вернул фьючерсные лимиты с FirmId={firm_id}, TradeAccountId={trade_account_id}. Проверьте правильность значений')
                return None
        # Для остальных фирм
        money_limits = self.store.provider.GetMoneyLimits()['data']  # Все денежные лимиты (остатки на счетах)
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
                return float(self.store.provider.GetFuturesLimit(firm_id, trade_account_id, 0, 'SUR')['data']['cbplused'])  # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                return None
        # Для остальных фирм
        pos_value = 0  # Стоимость позиций по счету
        for dataname in list(self.positions.keys()):  # Пробегаемся по копии позиций (чтобы не было ошибки при изменении позиций)
            class_code, sec_code = self.store.data_name_to_class_sec_code(dataname)  # По названию тикера получаем код площадки и код тикера
            last_price = float(self.store.provider.GetParamEx(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
            last_price = self.store.quik_to_bt_price(class_code, sec_code, last_price)  # Для рынка облигаций последнюю цену сделки умножаем на 10
            pos = self.positions[dataname]  # Получаем позицию по тикеру
            pos_value += pos.size * last_price  # Добавляем стоимость позиции
        return pos_value  # Стоимость позиций по счету

    def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок"""
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные свойства из брокера, в т.ч. ClientCode, TradeAccountId, StopOrderKind
        class_code, sec_code = self.store.data_name_to_class_sec_code(data._name)  # Из названия тикера получаем код площадки и тикера
        order.addinfo(ClassCode=class_code, SecCode=sec_code)  # Код площадки ClassCode и тикера SecCode
        si = self.store.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (min_price_step, scale)
        if not si:  # Если тикер не найден
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Тикер не найден')
            order.reject(self)  # то отменяем заявку (статус Order.Rejected)
            return order  # Возвращаем отмененную заявку
        order.addinfo(MinPriceStep=float(si['min_price_step']))  # Минимальный шаг цены
        order.addinfo(Slippage=float(si['min_price_step']) * self.store.p.StopSteps)  # Размер проскальзывания в деньгах Slippage
        order.addinfo(Scale=int(si['scale']))  # Кол-во значащих цифр после запятой Scale
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                print(f'Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Родительская заявка не найдена')
                order.reject(self)  # то отменяем заявку (статус Order.Rejected)
                return order  # Возвращаем отмененную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self.place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим

    def place_order(self, order):
        """Отправка заявки (транзакции) на биржу"""
        class_code = order.info['ClassCode']  # Код площадки
        sec_code = order.info['SecCode']  # Код тикера
        size = abs(self.store.size_to_lots(class_code, sec_code, order.size))  # Размер позиции в лотах. В QUIK всегда передается положительный размер лота
        price = order.price  # Цена заявки
        if not price:  # Если цена не указана для рыночных заявок
            price = 0.00  # Цена рыночной заявки должна быть нулевой (кроме фьючерсов)
        slippage = order.info['Slippage']  # Размер проскальзывания в деньгах
        if slippage.is_integer():  # Целое значение проскальзывания мы должны отправлять без десятичных знаков
            slippage = int(slippage)  # поэтому, приводим такое проскальзывание к целому числу
        if order.exectype == Order.Market:  # Для рыночных заявок
            if class_code == 'SPBFUT':  # Для рынка фьючерсов
                last_price = float(self.store.provider.GetParamEx(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
                price = last_price + slippage if order.isbuy() else last_price - slippage  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
        else:  # Для остальных заявок
            price = self.store.bt_to_quik_price(class_code, sec_code, price)  # Переводим цену из BackTrader в QUIK
        scale = order.info['Scale']  # Кол-во значащих цифр после запятой
        price = round(price, scale)  # Округляем цену до кол-ва значащих цифр
        if price.is_integer():  # Целое значение цены мы должны отправлять без десятичных знаков
            price = int(price)  # поэтому, приводим такую цену к целому числу
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLIENT_CODE': order.info['ClientCode'],  # Код клиента. Для фьючерсов его нет
            'ACCOUNT': order.info['TradeAccountId'],  # Счет
            'CLASSCODE': class_code,  # Код площадки
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(size)}  # Кол-во в лотах
        if order.exectype in [Order.Stop, Order.StopLimit]:  # Для стоп заявок
            transaction['ACTION'] = 'NEW_STOP_ORDER'  # Новая стоп заявка
            transaction['STOPPRICE'] = str(price)  # Стоп цена срабатывания
            plimit = order.pricelimit  # Лимитная цена исполнения
            if plimit:  # Если задана лимитная цена исполнения
                plimit = self.store.bt_to_quik_price(class_code, sec_code, plimit)  # Переводим цену из BackTrader в QUIK
                limit_price = round(plimit, scale)  # то ее и берем, округлив цену до кол-ва значащих цифр
            elif order.isbuy():  # Если цена не задана, и покупаем
                limit_price = round(price + slippage, scale)  # то будем покупать по большей цене в размер проскальзывания
            else:  # Если цена не задана, и продаем
                limit_price = round(price - slippage, scale)  # то будем продавать по меньшей цене в размер проскальзывания
            expiry_date = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiry_date = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                expiry_date = order.valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction['EXPIRY_DATE'] = expiry_date  # Срок действия стоп заявки
            if order.info['StopOrderKind'] == 'TAKE_PROFIT_STOP_ORDER':  # Если тип стоп заявки это тейк профит
                min_price_step = order.info['MinPriceStep']  # Минимальный шаг цены
                transaction['STOP_ORDER_KIND'] = order.info['StopOrderKind']  # Тип заявки TAKE_PROFIT_STOP_ORDER
                transaction['SPREAD_UNITS'] = 'PRICE_UNITS'  # Единицы измерения защитного спрэда в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['SPREAD'] = f'{min_price_step:.{scale}f}'  # Размер защитного спрэда. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
                transaction['OFFSET_UNITS'] = 'PRICE_UNITS'  # Единицы измерения отступа в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['OFFSET'] = f'{min_price_step:.{scale}f}'  # Размер отступа. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
            else:  # Для обычных стоп заявок
                transaction['PRICE'] = str(limit_price)  # Лимитная цена исполнения
        else:  # Для рыночных или лимитных заявок
            transaction['ACTION'] = 'NEW_ORDER'  # Новая рыночная или лимитная заявка
            transaction['TYPE'] = 'L' if order.exectype == Order.Limit else 'M'  # L = лимитная заявка (по умолчанию), M = рыночная заявка
        response = self.store.provider.SendTransaction(transaction)  # Отправляем транзакцию на биржу
        order.submit(self)  # Отправляем заявку на биржу (статус Order.Submitted)
        if response['cmd'] == 'lua_transaction_error':  # Если возникла ошибка при постановке заявки на уровне QUIK
            print(f'Ошибка отправки заявки в QUIK {response["data"]["CLASSCODE"]}.{response["data"]["SECCODE"]} {response["lua_error"]}')  # то заявка не отправляется на биржу, выводим сообщение об ошибке
            order.reject(self)  # Отклоняем заявку (Order.Rejected)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if order.ref not in self.orders:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        order_num = order.info['order_num']  # Номер заявки на бирже
        class_code, sec_code = self.store.data_name_to_class_sec_code(order.data._name)  # По названию тикера получаем код площадки и код тикера
        is_stop = order.exectype in [Order.Stop, Order.StopLimit] and \
            isinstance(self.store.provider.GetOrderByNumber(order_num)['data'], int)  # Задана стоп заявка и лимитная заявка не выставлена
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
        self.store.provider.SendTransaction(transaction)  # Отправляем транзакцию на биржу
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

    def on_trans_reply(self, data):
        """Обработчик события ответа на транзакцию пользователя"""
        self.logger.debug(f'on_trans_reply: data={data}')
        qk_trans_reply = data['data']  # Ответ на транзакцию
        order_num = int(qk_trans_reply['order_num'])  # Номер заявки на бирже
        trans_id = int(qk_trans_reply['trans_id'])  # Номер транзакции заявки
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. Номер транзакции 0. Выход')
            return  # не обрабатываем, пропускаем
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. Номер транзакции {trans_id} был выставлен не из торговой системы. Выход')
            return  # не обрабатываем, пропускаем
        order: Order = self.orders[trans_id]  # Ищем заявку по номеру транзакции
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже
        self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. order={order}')
        # TODO Есть поле flags, но оно не документировано. Лучше вместо текстового результата транзакции разбирать по нему
        result_msg = str(qk_trans_reply['result_msg']).lower()  # По результату исполнения транзакции (очень плохое решение)
        status = int(qk_trans_reply['status'])  # Статус транзакции
        if status == 15 or 'зарегистрирован' in result_msg:  # Если пришел ответ по новой заявке
            self.logger.debug(f'on_trans_reply: Перевод заявки с номером {order_num} в статус принята на бирже (Order.Accepted)')
            order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        elif 'снят' in result_msg:  # Если пришел ответ по отмене существующей заявки
            try:  # TODO В BT очень редко при order.cancel() возникает ошибка:
                #    order.py, line 487, in cancel
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                self.logger.debug(f'on_trans_reply: Перевод заявки с номером {order_num} в статус отменена (Order.Canceled)')
                order.cancel()  # Отменяем существующую заявку (Order.Canceled)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Canceled  # все равно ставим статус заявки Order.Canceled
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):  # Транзакция не выполнена (ошибка заявки):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if status == 4 and 'не найдена заявка' in result_msg or \
               status == 5 and 'не можете снять' in result_msg or 'превышен лимит' in result_msg:
                self.logger.debug(f'on_trans_reply: Ошибка заявки с номером {order_num}. Выход')
                return  # то заявку не отменяем, выходим, дальше не продолжаем
            try:  # TODO В BT очень редко при order.reject() возникает ошибка:
                #    order.py, line 480, in reject
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                self.logger.debug(f'on_trans_reply: Перевод заявки с номером {order_num} в статус отклонена (Order.Rejected)')
                order.reject(self)  # Отклоняем заявку (Order.Rejected)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Rejected  # все равно ставим статус заявки Order.Rejected
        elif status == 6:  # Транзакция не прошла проверку лимитов сервера QUIK
            try:  # TODO В BT очень редко при order.margin() возникает ошибка:
                #    order.py, line 492, in margin
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                self.logger.debug(f'on_trans_reply: Перевод заявки с номером {order_num} в статус не прошла проверку лимитов (Order.Margin)')
                order.margin()  # Для заявки не хватает средств (Order.Margin)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Margin  # все равно ставим статус заявки Order.Margin
        self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:  # Если новая заявка не зарегистрирована
            self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. Проверка связанных и родительских/дочерних заявок')
            self.oco_pc_check(order)  # то проверяем связанные и родительскую/дочерние заявки (Canceled, Rejected, Margin)
        self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. Выход')

    def on_trade(self, data):
        """Обработчик события получения новой / изменения существующей сделки.
        Выполняется до события изменения существующей заявки. Нужен для определения цены исполнения заявок.
        """
        self.logger.debug(f'on_trade: data={data}')
        qk_trade = data['data']  # Сделка в QUIK
        order_num = int(qk_trade['order_num'])  # Номер заявки на бирже
        qk_order = self.store.provider.GetOrderByNumber(order_num)['data']  # По номеру заявки в сделке пробуем получить заявку с биржи
        if isinstance(qk_order, int):  # Если заявка не найдена, то в ответ получаем целое число номера заявки. Возможно заявка есть, но она не успела прийти к брокеру
            self.logger.debug(f'on_trade: Заявка с номером {order_num} не найдена на бирже с 1-ой попытки. Через 3 с будет 2-ая попытка')
            time.sleep(3)  # Ждем 3 секунды, пока заявка не придет к брокеру
            qk_order = self.store.provider.GetOrderByNumber(order_num)['data']  # Снова пробуем получить заявку с биржи по ее номеру
            if isinstance(qk_order, int):  # Если заявка так и не была найдена
                self.logger.debug(f'on_trade: Заявка с номером {order_num} не найдена на бирже со 2-ой попытки. Выход')
                return  # то выходим, дальше не продолжаем
        self.logger.debug(f'on_trade: Заявка с номером {order_num} qk_order={qk_order}')
        try:  # Бывает, что номер транзакции не число. Проверяем
            trans_id = int(qk_order['trans_id'])  # Получаем номер транзакции из заявки с биржи
        except ValueError:
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Номер транзакции {qk_order["trans_id"]} не является целым числом. Выход')
            return  # выходим, дальше не продолжаем
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Номер транзакции 0. Выход')
            return  # выходим, дальше не продолжаем
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Номер транзакции {trans_id} был выставлен не из торговой системы. Выход')
            return  # выходим, дальше не продолжаем
        order: Order = self.orders[trans_id]  # Ищем заявку по номеру транзакции
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        self.logger.debug(f'on_trade: Заявка с номером {order_num}. order={order}')
        class_code = qk_trade['class_code']  # Код площадки
        sec_code = qk_trade['sec_code']  # Код тикера
        dataname = self.store.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
        self.logger.debug(f'on_trade: Заявка с номером {order_num}. dataname={dataname}')
        trade_num = int(qk_trade['trade_num'])  # Номер сделки (дублируется 3 раза)
        if dataname not in self.trade_nums.keys():  # Если это первая сделка по тикеру
            self.trade_nums[dataname] = []  # то ставим пустой список сделок
        elif trade_num in self.trade_nums[dataname]:  # Если номер сделки есть в списке (фильтр для дублей)
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Номер сделки {trade_num} есть в списке сделок (дубль). Выход')
            return  # то выходим, дальше не продолжаем
        self.trade_nums[dataname].append(trade_num)  # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        size = int(qk_trade['qty'])  # Абсолютное кол-во
        if self.p.Lots:  # Если входящий остаток в лотах
            size = self.store.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
        if qk_trade['flags'] & 0b100 == 0b100:  # Если сделка на продажу (бит 2)
            size *= -1  # то кол-во ставим отрицательным
        price = self.store.quik_to_bt_price(class_code, sec_code, float(qk_trade['price']))  # Переводим цену исполнения за лот в цену исполнения за штуку
        self.logger.debug(f'on_trade: Заявка с номером {order_num}. size={size}, price={price}')
        try:  # TODO Очень редко возникает ошибка:
            #    linebuffer.py, line 163, in __getitem__
            #    return self.array[self.idx + ago]
            #    IndexError: array index out of range
            dt = order.data.datetime[0]  # Дата и время исполнения заявки. Последняя известная
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Дата/время исполнения заявки по бару {dt}')
        except (KeyError, IndexError):  # При ошибке
            dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на бирже из локального
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Дата/время исполнения заявки по текущему {dt}')
        pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если заявка исполнена частично (осталось что-то к исполнению)
            self.logger.debug(f'on_trade: Заявка с номером {order_num} исполнилась частично. Остаток к исполнения {order.executed.remsize}')
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                self.logger.debug(f'on_trade: Перевод заявки с номером {order_num} в статус частично исполнена (Order.Partial)')
                order.partial()  # Переводим заявку в статус Order.Partial
                self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если заявка исполнена полностью (ничего нет к исполнению)
            self.logger.debug(f'on_trade: Перевод заявки с номером {order_num} в статус полностью исполнена (Order.Completed)')
            order.completed()  # Переводим заявку в статус Order.Completed
            self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Проверка связанных и родительских/дочерних заявок')
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
        self.logger.debug(f'on_trade: Заявка с номером {order_num}. Выход')
