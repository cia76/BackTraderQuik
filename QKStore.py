import collections
from datetime import datetime, date
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
        if cls._singleton is None:  # Если класса нет в экземплярах класса
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

    MarketTimeZone = timezone('Europe/Moscow')  # Рынок работает по московскому времени

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
        self.qpProvider = QuikPy(Host=self.p.Host, RequestsPort=self.p.RequestsPort, CallbacksPort=self.p.CallbacksPort)  # Вызываем конструктор QuikPy с адресом хоста и портами по умолчанию
        self.classCodes = self.qpProvider.GetClassesList()['data']  # Список классов. В некоторых таблицах тикер указывается без кода класса
        self.subscribedSymbols = []  # Список подписанных тикеров/интервалов
        self.securityInfoList = []  # Кэш параметров тикеров
        self.newBars = []  # Новые бары по подписке из QUIK
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на рынок
        self.orderNums = {}  # Словарь заявок на рынке. Индекс - номер транзакции, значение - номер заявки на рынке
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)
        self.ocos = {}  # Список связанных заявок (One Cancel Others)

    def start(self):
        self.qpProvider.OnNewCandle = self.OnNewCandle  # Обработчик новых баров по подписке из QUIK

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.qpProvider.OnNewCandle = self.qpProvider.DefaultHandler  # Возвращаем обработчик по умолчанию
        self.qpProvider.CloseConnectionAndThread()  # Закрываем соединение для запросов и поток обработки функций обратного вызова

    # Функции конвертации

    def DataNameToClassSecCode(self, dataname):
        """Код площадки и код тикера из названия тикера (с кодом площадки или без него)"""
        symbolParts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbolParts) >= 2:  # Если тикер задан в формате <Код площадки>.<Код тикера>
            classCode = symbolParts[0]  # Код площадки
            secCode = '.'.join(symbolParts[1:])  # Код тикера
        else:  # Если тикер задан без площадки
            classCode = self.qpProvider.GetSecurityClass(self.classCodes, dataname)['data']  # Получаем код площадки по коду инструмента из имеющихся классов
            secCode = dataname  # Код тикера
        return classCode, secCode  # Возвращаем код площадки и код тикера

    def ClassSecCodeToDataName(self, ClassCode, SecCode):
        """Название тикера из кода площадки и кода тикера"""
        return f'{ClassCode}.{SecCode}'

    def GetSecurityInfo(self, ClassCode, SecCode):
        """Параметры тикера из кэша / по запросу"""
        si = [securityInfo for securityInfo in self.securityInfoList if securityInfo['class_code'] == ClassCode and securityInfo['sec_code'] == SecCode]  # Ищем в кэше параметры тикера
        if len(si) == 0:  # Если параметры тикера не найдены в кэше
            si = self.qpProvider.GetSecurityInfo(ClassCode, SecCode)  # то делаем запрос параметров тикера
            if 'data' not in si:  # Если ответ не пришел (возникла ошибка). Например, для опциона
                print(f'Информация о {ClassCode}.{SecCode} не найдена')
                return None  # то выходим, дальше не продолжаем
            self.securityInfoList.append(si['data'])  # Добавляем полученные параметры тикера в кэш
            return si['data']  # Возвращаем их
        else:  # Если параметры тикера найдены в кэше
            return si[0]  # то возвращаем первый элемент

    def SizeToLots(self, ClassCode, SecCode, Size: int):
        """Из штук в лоты"""
        si = self.GetSecurityInfo(ClassCode, SecCode)  # Получаем параметры тикера (lot_size)
        if si is None:  # Если тикер не найден
            return Size  # то кол-во не изменяется
        securityLot = int(si['lot_size'])  # Размер лота тикера
        return int(Size / securityLot) if securityLot > 0 else Size  # Если задан лот, то переводим

    def LotsToSize(self, ClassCode, SecCode, Lots: int):
        """Из лотов в штуки"""
        si = self.GetSecurityInfo(ClassCode, SecCode)  # Получаем параметры тикера (lot_size)
        if si is None:  # Если тикер не найден
            return Lots  # то лот не изменяется
        securityLot = int(si['lot_size'])  # Размер лота тикера
        return Lots * securityLot if securityLot > 0 else Lots  # Если задан лот, то переводим

    def BTToQKPrice(self, ClassCode, SecCode, Price: float):
        """Перевод цен из BackTrader в QUIK"""
        if ClassCode == 'TQOB':  # Для рынка облигаций
            return Price / 10  # цену делим на 10
        if ClassCode == 'SPBFUT':  # Для рынка фьючерсов
            si = self.GetSecurityInfo(ClassCode, SecCode)  # Получаем параметры тикера (lot_size)
            if si is None:  # Если тикер не найден
                return Price  # то цена не изменяется
            securityLot = int(si['lot_size'])  # Размер лота тикера
            if securityLot > 0:  # Если лот задан
                return Price * securityLot  # то цену умножаем на лот
        return Price  # В остальных случаях цена не изменяется

    def QKToBTPrice(self, ClassCode, SecCode, Price: float):
        """Перевод цен из QUIK в BackTrader"""
        if ClassCode == 'TQOB':  # Для рынка облигаций
            return Price * 10  # цену умножаем на 10
        if ClassCode == 'SPBFUT':  # Для рынка фьючерсов
            si = self.GetSecurityInfo(ClassCode, SecCode)  # Получаем параметры тикера (lot_size)
            if si is None:  # Если тикер не найден
                return Price  # то цена не изменяется
            securityLot = int(si['lot_size'])  # Размер лота тикера
            if securityLot > 0:  # Если лот задан
                return Price / securityLot  # то цену делим на лот
        return Price  # В остальных случаях цена не изменяется

    # QKBroker: Функции

    def GetPositions(self, ClientCode, FirmId, LimitKind, Lots, IsFutures=False):
        """
        Все активные позиции по счету
        Для фьючерсных счетов нужно установить параметр IsFutures=True
        """
        if IsFutures:  # Для фьючерсов свои расчеты
            futuresHoldings = self.qpProvider.GetFuturesHoldings()['data']  # Все фьючерсные позиции
            activeFuturesHoldings = [futuresHolding for futuresHolding in futuresHoldings if futuresHolding['totalnet'] != 0]  # Активные фьючерсные позиции
            for activeFuturesHolding in activeFuturesHoldings:  # Пробегаемся по всем активным фьючерсным позициям
                classCode = 'SPBFUT'  # Код площадки
                secCode = activeFuturesHolding['sec_code']  # Код тикера
                dataname = self.ClassSecCodeToDataName(classCode, secCode)  # Получаем название тикера по коду площадки и коду тикера
                size = activeFuturesHolding['totalnet']  # Кол-во
                if Lots:  # Если входящий остаток в лотах
                    size = self.LotsToSize(classCode, secCode, size)  # то переводим кол-во из лотов в штуки
                price = float(activeFuturesHolding['avrposnprice'])  # Цена приобретения
                price = self.QKToBTPrice(classCode, secCode, price)  # Переводим цену приобретения за лот в цену приобретения за штуку
                self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций
        else:  # Для остальных фирм
            depoLimits = self.qpProvider.GetAllDepoLimits()['data']  # Все лимиты по бумагам (позиции по инструментам)
            accountDepoLimits = [depoLimit for depoLimit in depoLimits  # Бумажный лимит
                                if depoLimit['client_code'] == ClientCode and  # выбираем по коду клиента
                                depoLimit['firmid'] == FirmId and  # фирме
                                depoLimit['limit_kind'] == LimitKind and  # дню лимита
                                depoLimit['currentbal'] != 0]  # только открытые позиции
            for firmKindDepoLimit in accountDepoLimits:  # Пробегаемся по всем позициям
                dataname = firmKindDepoLimit['sec_code']  # В позициях код тикера указывается без кода площадки
                classCode, secCode = self.DataNameToClassSecCode(dataname)  # По коду тикера без площадки получаем код площадки и код тикера
                size = int(firmKindDepoLimit['currentbal'])  # Кол-во
                if Lots:  # Если входящий остаток в лотах
                    size = self.LotsToSize(classCode, secCode, size)  # то переводим кол-во из лотов в штуки
                price = float(firmKindDepoLimit['wa_position_price'])  # Цена приобретения
                price = self.QKToBTPrice(classCode, secCode, price)  # Для рынка облигаций цену приобретения умножаем на 10
                dataname = self.ClassSecCodeToDataName(classCode, secCode)  # Получаем название тикера по коду площадки и коду тикера
                self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций

    def GetMoneyLimits(self, ClientCode, FirmId, TradeAccountId, LimitKind, CurrencyCode, IsFutures=False):
        """
        Свободные средства по счету
        Для фьючерсных счетов нужно установить параметр IsFutures=True
        """
        if IsFutures:  # Для фьючерсов свои расчеты
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход включает Биржевые сборы
            # Тек.чист.поз. = Заблокированное ГО под открытые позиции
            # План.чист.поз. = На какую сумму можете открыть еще позиции
            try:
                futuresLimit = self.qpProvider.GetFuturesLimit(FirmId, TradeAccountId, 0, 'SUR')['data']  # Фьючерсные лимиты
                return float(futuresLimit['cbplimit']) + float(futuresLimit['varmargin']) + float(futuresLimit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                print(f'QUIK не вернул фьючерсные лимиты с FirmId={FirmId}, TradeAccountId={TradeAccountId}. Проверьте правильность значений')
                return None
        # Для остальных фирм
        money_limits = self.qpProvider.GetMoneyLimits()['data']  # Все денежные лимиты (остатки на счетах)
        if len(money_limits) == 0:  # Если денежных лимитов нет
            print('QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return None
        cash = [moneyLimit for moneyLimit in money_limits  # Из всех денежных лимитов
                if moneyLimit['client_code'] == ClientCode and  # выбираем по коду клиента
                moneyLimit['firmid'] == FirmId and  # фирме
                moneyLimit['limit_kind'] == LimitKind and  # дню лимита
                moneyLimit["currcode"] == CurrencyCode]  # и валюте
        if len(cash) != 1:  # Если ни один денежный лимит не подходит
            print(f'Денежный лимит не найден с ClientCode={ClientCode}, FirmId={FirmId}, LimitKind={LimitKind}, CurrencyCode={CurrencyCode}. Проверьте правильность значений')
            # print(f'Полученные денежные лимиты: {money_limits}')  # Для отладки, если нужно разобраться, что указано неверно
            return None
        return float(cash[0]['currentbal'])  # Денежный лимит (остаток) по счету

    def GetPositionsLimits(self, FirmId, TradeAccountId, IsFutures=False):
        """
        Стоимость позиций по счету
        Для фьючерсных счетов нужно установить параметр IsFutures=True
        """
        if IsFutures:  # Для фьючерсов свои расчеты
            try:
                return float(self.qpProvider.GetFuturesLimit(FirmId, TradeAccountId, 0, 'SUR')['data']['cbplused'])  # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                return None
        # Для остальных фирм
        posValue = 0  # Стоимость позиций по счету
        for dataname in list(self.positions.keys()):  # Пробегаемся по копии позиций (чтобы не было ошибки при изменении позиций)
            classCode, secCode = self.DataNameToClassSecCode(dataname)  # По названию тикера получаем код площадки и код тикера
            lastPrice = float(self.qpProvider.GetParamEx(classCode, secCode, 'LAST')['data']['param_value'])  # Последняя цена сделки
            lastPrice = self.QKToBTPrice(classCode, secCode, lastPrice)  # Для рынка облигаций последнюю цену сделки умножаем на 10
            pos = self.positions[dataname]  # Получаем позицию по тикеру
            posValue += pos.size * lastPrice  # Добавляем стоимость позиции
        return posValue  # Стоимость позиций по счету

    def PlaceOrder(self, order):
        """Отправка заявки (транзакции) на рынок"""
        classCode = order.info['ClassCode']  # Код площадки
        secCode = order.info['SecCode']  # Код тикера
        size = abs(self.SizeToLots(classCode, secCode, order.size))  # Размер позиции в лотах. В QUIK всегда передает положительный размер лота
        price = order.price  # Цена заявки
        if price is None:  # Если цена не указана для рыночных заявок
            price = 0.00  # Цена рыночной заявки должна быть нулевой (кроме фьючерсов)
        slippage = order.info['Slippage']  # Размер проскальзывания в деньгах
        if slippage.is_integer():  # Целое значение проскальзывания мы должны отправлять без десятичных знаков
            slippage = int(slippage)  # поэтому, приводим такое проскальзывание к целому числу
        if order.exectype == Order.Market:  # Для рыночных заявок
            if classCode == 'SPBFUT':  # Для рынка фьючерсов
                lastPrice = float(self.qpProvider.GetParamEx(classCode, secCode, 'LAST')['data']['param_value'])  # Последняя цена сделки
                price = lastPrice + slippage if order.isbuy() else lastPrice - slippage  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
        else:  # Для остальных заявок
            price = self.BTToQKPrice(classCode, secCode, price)  # Переводим цену из BackTrader в QUIK
        scale = order.info['Scale']  # Кол-во значащих цифр после запятой
        price = round(price, scale)  # Округляем цену до кол-ва значащих цифр
        if price.is_integer():  # Целое значение цены мы должны отправлять без десятичных знаков
            price = int(price)  # поэтому, приводим такую цену к целому числу
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLIENT_CODE': order.info['ClientCode'],  # Код клиента. Для фьючерсов его нет
            'ACCOUNT': order.info['TradeAccountId'],  # Счет
            'CLASSCODE': classCode,  # Код площадки
            'SECCODE': secCode,  # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(size)}  # Кол-во в лотах
        if order.exectype in [Order.Stop, Order.StopLimit]:  # Для стоп заявок
            transaction['ACTION'] = 'NEW_STOP_ORDER'  # Новая стоп заявка
            transaction['STOPPRICE'] = str(price)  # Стоп цена срабатывания
            plimit = order.pricelimit  # Лимитная цена исполнения
            if plimit is not None:  # Если задана лимитная цена исполнения
                limitPrice = round(plimit, scale)  # то ее и берем, округлив цену до кол-ва значащих цифр
            elif order.isbuy():  # Если цена не задана, и покупаем
                limitPrice = price + slippage  # то будем покупать по большей цене в размер проскальзывания
            else:  # Если цена не задана, и продаем
                limitPrice = price - slippage  # то будем продавать по меньшей цене в размер проскальзывания
            expiryDate = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiryDate = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                expiryDate = order.valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction['EXPIRY_DATE'] = expiryDate  # Срок действия стоп заявки
            if order.info['StopOrderKind'] == 'TAKE_PROFIT_STOP_ORDER':  # Если тип стоп заявки это тейк профит
                minPriceStep = order.info['MinPriceStep']  # Минимальный шаг цены
                transaction['STOP_ORDER_KIND'] = order.info['StopOrderKind']  # Тип заявки TAKE_PROFIT_STOP_ORDER
                transaction['SPREAD_UNITS'] = 'PRICE_UNITS'  # Единицы измерения защитного спрэда в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['SPREAD'] = f'{minPriceStep:.{scale}f}'  # Размер защитного спрэда. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
                transaction['OFFSET_UNITS'] = 'PRICE_UNITS'  # Единицы измерения отступа в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['OFFSET'] = f'{minPriceStep:.{scale}f}'  # Размер отступа. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
            else:  # Для обычных стоп заявок
                transaction['PRICE'] = str(limitPrice)  # Лимитная цена исполнения
        else:  # Для рыночных или лимитных заявок
            transaction['ACTION'] = 'NEW_ORDER'  # Новая рыночная или лимитная заявка
            transaction['TYPE'] = 'L' if order.exectype == Order.Limit else 'M'  # L = лимитная заявка (по умолчанию), M = рыночная заявка
        response = self.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на рынок
        order.submit(self)  # Переводим заявку в статус Order.Submitted
        if response['cmd'] == 'lua_transaction_error':  # Если возникла ошибка при постановке заявки на уровне QUIK
            print(f'Ошибка отправки заявки в QUIK {response["data"]["CLASSCODE"]}.{response["data"]["SECCODE"]} {response["lua_error"]}')  # то заявка не отправляется на рынок, выводим сообщение об ошибке
            order.reject()  # Переводим заявку в статус Order.Reject
        self.orders[order.ref] = order  # Сохраняем в списке заявок, отправленных на рынок
        return order  # Возвращаем заявку

    def CancelOrder(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if not self.orders.get(order.ref, False):  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        if order.ref not in self.orderNums:  # Если заявки нет в словаре заявок на рынке
            return  # то выходим, дальше не продолжаем
        orderNum = self.orderNums[order.ref]  # Номер заявки на рынке
        classCode, secCode = self.DataNameToClassSecCode(order.data._dataname)  # По названию тикера получаем код площадки и код тикера
        isStop = order.exectype in [Order.Stop, Order.StopLimit] and \
            isinstance(self.qpProvider.GetOrderByNumber(orderNum)['data'], int)  # Задана стоп заявка и лимитная заявка не выставлена
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': classCode,  # Код площадки
            'SECCODE': secCode}  # Код тикера
        if isStop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'  # Будем удалять стоп заявку
            transaction['STOP_ORDER_KEY'] = str(orderNum)  # Номер стоп заявки на рынке
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'  # Будем удалять лимитную заявку
            transaction['ORDER_KEY'] = str(orderNum)  # Номер заявки на рынке
        self.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на рынок
        return order  # В список уведомлений ничего не добавляем. Ждем события OnTransReply

    def OCOPCCheck(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        for orderRef, ocoRef in self.ocos.items():  # Пробегаемся по списку связанных заявок
            if ocoRef == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.CancelOrder(self.orders[orderRef])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            ocoRef = self.ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.CancelOrder(self.orders[ocoRef])  # отменяем связанную заявку

        if order.parent is None and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent is not None:  # Пропускаем первую (родительскую) заявку
                    self.PlaceOrder(child)  # Отправляем дочернюю заявку на рынок
        elif order.parent is not None:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent is not None and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.CancelOrder(child)  # Отменяем дочернюю заявку

    # QKBroker: Обработка событий подключения к QUIK / отключения от QUIK

    def OnConnected(self, data):
        dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на рынке из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Connected')
        self.isConnected = True  # QUIK подключен к серверу брокера
        print(f'Проверка подписки тикеров ({len(self.subscribedSymbols)})')
        for subscribedSymbol in self.subscribedSymbols:  # Пробегаемся по всем подписанным тикерам
            classCode = subscribedSymbol['class']  # Код площадки
            secCode = subscribedSymbol['sec']  # Код тикера
            interval = subscribedSymbol['interval']  # Временной интервал
            print(f'{classCode}.{secCode} на интервале {interval}', end=' ')
            if not self.qpProvider.IsSubscribed(classCode, secCode, interval)['data']:  # Если нет подписки на тикер/интервал
                self.qpProvider.SubscribeToCandles(classCode, secCode, interval)  # то переподписываемся
                print('нет подписки. Отправлен запрос на новую подписку')
            else:  # Если подписка была, то переподписываться не нужно
                print('есть подписка')

    def OnDisconnected(self, data):
        if not self.isConnected:  # Если QUIK отключен от сервера брокера
            return  # то не нужно дублировать сообщение, выходим, дальше не продолжаем
        dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на рынке из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Disconnected')
        self.isConnected = False  # QUIK отключен от сервера брокера

    # QKData: Обработка событий получения новых баров

    def OnNewCandle(self, data):
        self.newBars.append(data['data'])  # Добавляем новый бар в список новых баров
