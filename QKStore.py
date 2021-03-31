import collections
from datetime import datetime, date
from pytz import timezone

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass
from backtrader import Order, BuyOrder, SellOrder
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
        ('CallbacksPort', 34131))  # Номер порта для получения событий

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    MarketTimeZone = timezone('Europe/Moscow')  # Биржа работает по московскому времени
    StopSteps = 10  # Размер в минимальных шагах цены инструмента для исполнения стоп заявок

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns `DataCls` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered `BrokerCls`"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(QKStore, self).__init__()
        self.notifs = collections.deque()  # Уведомления хранилища
        self.isConnected = True  # Считаем, что изначально QUIK подключен к серверу брокера
        self.qpProvider = QuikPy(Host=self.p.Host, RequestsPort=self.p.RequestsPort, CallbacksPort=self.p.CallbacksPort)  # Вызываем конструктор QuikPy с адресом хоста и портами по умолчанию
        self.classCodes = self.qpProvider.GetClassesList()['data']  # Список классов. В некоторых таблицах тикер указывается без кода класса
        self.securityInfoList = []  # Кэш параметров тикеров

        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок
        self.newTransId = 1  # Следующий внутренний номер транзакции заявки (задается пользователем)
        self.orderNums = {}  # Словарь заявок на бирже. Индекс - номер транзакции, значение - номер заявки на бирже
        self.ocos = {}  # Список связанных заявок

    def start(self):
        pass

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.qpProvider.CloseConnectionAndThread()  # то закрываем соединение для запросов и поток обработки функций обратного вызова

    # Функции конвертации

    def DataNameToClassSecCode(self, dataname):
        """Код площадки и код тикера из названия тикера (с кодом площадки или без него)"""
        symbolParts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbolParts) == 2:  # Если тикер задан в формате <Код площадки>.<Код тикера>
            return symbolParts[0], symbolParts[1]  # то возвращаем код площадки и код тикера

        classCode = self.qpProvider.GetSecurityClass(self.classCodes, dataname)['data']  # Получаем класс по коду инструмента из заданных классов
        return classCode, dataname  # Возвращаем код площадки и код тикера

    def ClassSecCodeToDataName(self, ClassCode, SecCode):
        """Название тикера из кода площадки и кода тикера"""
        return f'{ClassCode}.{SecCode}'

    def GetSecurityInfo(self, ClassCode, SecCode):
        """Параметры тикера из кэша / по запросу"""
        si = [securityInfo for securityInfo in self.securityInfoList if securityInfo['class_code'] == ClassCode and securityInfo['sec_code'] == SecCode]  # Ищем в кэше параметры тикера
        if len(si) == 0:  # Если параметры тикера не найдены в кэше
            si = self.qpProvider.GetSecurityInfo(ClassCode, SecCode)['data']  # то делаем запрос параметров тикера
            self.securityInfoList.append(si)  # Добавляем полученные параметры тикера в кэш
            return si  # Возвращаем их
        else:  # Если параметры тикера найдены в кэше
            return si[0]  # то возвращаем первый элемент

    def SizeToLots(self, ClassCode, SecCode, Size: int):
        """Из штук в лоты"""
        securityLot = int(self.GetSecurityInfo(ClassCode, SecCode)['lot_size'])  # Размер лота тикера
        return int(Size / securityLot) if securityLot > 0 else Size  # Если задан лот, то переводим

    def LotsToSize(self, ClassCode, SecCode, Lots: int):
        """Из лотов в штуки"""
        securityLot = int(self.GetSecurityInfo(ClassCode, SecCode)['lot_size'])  # Размер лота тикера
        return Lots * securityLot if securityLot > 0 else Lots  # Если задан лот, то переводим

    def BTToQKPrice(self, ClassCode, SecCode, Price: float):
        """Перевод цен из BackTrader в QUIK"""
        if ClassCode == 'TQOB':  # Для рынка облигаций
            return Price / 10  # цену делим на 10

        if ClassCode == 'SPBFUT':  # Для рынка фьючерсов
            securityLot = int(self.GetSecurityInfo(ClassCode, SecCode)['lot_size'])  # Размер лота тикера
            if securityLot > 0:  # Если лот задан
                return Price * securityLot  # то цену умножаем на лот

        return Price  # В остальных случаях цена не изменяется

    def QKToBTPrice(self, ClassCode, SecCode, Price: float):
        """Перевод цен из QUIK в BackTrader"""
        if ClassCode == 'TQOB':  # Для рынка облигаций
            return Price * 10  # цену умножаем на 10

        if ClassCode == 'SPBFUT':  # Для рынка фьючерсов
            securityLot = int(self.GetSecurityInfo(ClassCode, SecCode)['lot_size'])  # Размер лота тикера
            if securityLot > 0:  # Если лот задан
                return Price / securityLot  # то цену делим на лот

        return Price  # В остальных случаях цена не изменяется

    # QKBroker

    def GetPositions(self, ClientCode, FirmId, LimitKind, Lots):
        """Все активные позиции по счету"""
        if FirmId == 'SPBFUT':  # Для фьючерсов свои расчеты
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

    def GetMoneyLimits(self, ClientCode, FirmId, TradeAccountId, LimitKind, CurrencyCode):
        """Свободные средства по счету"""
        if FirmId == 'SPBFUT':  # Для фьючерсов свои расчеты
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. - Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа - Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход - Включает Биржевые сборы
            # Тек.чист.поз. - Заблокированное ГО под открытые позиции
            # План.чист.поз. - На какую сумму можете открыть еще позиции
            try:
                futuresLimit = self.qpProvider.GetFuturesLimit(FirmId, TradeAccountId, 0, 'SUR')['data']
                return float(futuresLimit['cbplimit']) + float(futuresLimit['varmargin']) + float(futuresLimit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                return None
        # Для остальных фирм
        money_limits = self.qpProvider.GetMoneyLimits()['data']  # Все денежные лимиты (остатки на счетах)
        cash = [moneyLimit for moneyLimit in money_limits  # Первый денежный лимит
                if moneyLimit['client_code'] == ClientCode and  # выбираем по коду клиента
                moneyLimit['firmid'] == FirmId and  # фирме
                moneyLimit['limit_kind'] == LimitKind and  # дню лимита
                moneyLimit["currcode"] == CurrencyCode][0]  # валюте
        return float(cash['currentbal'])  # Денежный лимит (остаток) по счету

    def GetPositionsLimits(self, FirmId, TradeAccountId):
        """Стоимость позиций по счету"""
        if FirmId == 'SPBFUT':  # Для фьючерсов свои расчеты
            try:
                return float(self.qpProvider.GetFuturesLimit(FirmId, TradeAccountId, 0, 'SUR')['data']['cbplused'])  # Тек.чист.поз. (аблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                return None

        # Для остальных фирм
        posValue = 0  # Стоимость позиций по счету
        for dataname, pos in self.positions.items():  # Пробегаемся по всем открытым позициям
            classCode, secCode = self.DataNameToClassSecCode(dataname)  # По названию тикера получаем код площадки и код тикера
            lastPrice = float(self.qpProvider.GetParamEx(classCode, secCode, 'LAST')['data']['param_value'])  # Последняя цена сделки
            lastPrice = self.QKToBTPrice(classCode, secCode, lastPrice)  # Для рынка облигаций последнюю цену сделки умножаем на 10
            posValue += pos.size * lastPrice  # Добавляем стоимость позиции
        return posValue  # Стоимость позиций по счету

    def PlaceOrder(self, ClientCode, TradeAccountId, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, CommInfo=None, IsBuy=True, **kwargs):
        # TODO: Организовать работу группы заявок с 'parent' и 'transmit'
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, oco=oco) if IsBuy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, oco=oco)  # Заявка на покупку/продажу
        order.addinfo(**kwargs)  # Передаем все дополнительные параметры
        order.addcomminfo(CommInfo)  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        classCode, secCode = self.DataNameToClassSecCode(data._dataname)  # Из названия тикера получаем код площадки и тикера
        size = self.SizeToLots(classCode, secCode, size)  # Размер позиции в лотах
        if order.exectype == Order.Market:  # Для рыночных заявок
            if classCode == 'SPBFUT':  # Для рынка фьючерсов
                lastPrice = float(self.qpProvider.GetParamEx(classCode, secCode, 'LAST')['data']['param_value'])  # Последняя цена сделки
                price = lastPrice * 1.001 if IsBuy else lastPrice * 0.999  # Наихудшая цена (на 0.1% хуже последней цены). Все равно, заявка исполнится по рыночной цене
            else:  # Для остальных рынков
                price = 0  # Цена рыночной заявки должна быть нулевой
        else:  # Для остальных заявок
            price = self.BTToQKPrice(classCode, secCode, price)  # Переводим цену из BackTrader в QUIK
        scale = int(self.GetSecurityInfo(classCode, secCode)['scale'])  # Кол-во значащих цифр после запятой
        price = round(price, scale)  # Округляем цену до кол-ва значащих цифр
        if price.is_integer():  # Целое значение цены мы должны отправлять без десятичных знаков
            price = int(price)  # поэтому, приводим такую цену к целому числу
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(self.newTransId),  # Номер транзакции задается клиентом
            'CLIENT_CODE': ClientCode,  # Код клиента. Для фьючерсов его нет
            'ACCOUNT': TradeAccountId,  # Счет
            'CLASSCODE': classCode,  # Код площадки
            'SECCODE': secCode,  # Код тикера
            'OPERATION': 'B' if IsBuy else 'S',  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(size)}  # Кол-во в лотах
        if order.exectype in [Order.Stop, Order.StopLimit]:  # Для стоп заявок
            transaction['ACTION'] = 'NEW_STOP_ORDER'  # Новая стоп заявка
            transaction['STOPPRICE'] = str(price)  # Стоп цена срабатывания
            slippage = float(self.GetSecurityInfo(classCode, secCode)['min_price_step']) * self.StopSteps  # Размер проскальзывания в деньгах
            if slippage.is_integer():  # Целое значение проскальзывания мы должны отправлять без десятичных знаков
                slippage = int(slippage)  # поэтому, приводим такое проскальзывание к целому числу
            if plimit is not None:  # Если задана лимитная цена исполнения
                limitPrice = plimit  # то ее и берем
            elif IsBuy:  # Если цена не задана, и покупаем
                limitPrice = price + slippage  # то будем покупать по большей цене в размер проскальзывания
            else:  # Если цена не задана, и продаем
                limitPrice = price - slippage  # то будем продавать по меньшей цене в размер проскальзывания
            transaction['PRICE'] = str(limitPrice)  # Лимитная цена исполнения
            expiryDate = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiryDate = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(valid, date):  # Если заявка поставлена до даты
                expiryDate = valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction['EXPIRY_DATE'] = expiryDate  # Срок действия стоп заявки
        else:  # Для рыночных или лимитных заявок
            transaction['ACTION'] = 'NEW_ORDER'  # Новая рыночная или лимитная заявка
            transaction['TYPE'] = 'L' if order.exectype == Order.Limit else 'M'  # L = лимитная заявка (по умолчанию), M = рыночная заявка
        order.ref = self.newTransId  # Ставим номер транзакции в заявку
        self.newTransId += 1  # Увеличиваем номер транзакции для будущих заявок
        if oco is not None:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список родительских заявок
        self.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на рынок
        order.submit(self)  # Переводим заявку в статус Order.Submitted
        self.orders[order.ref] = order  # Сохраняем в списке заявок
        return order  # Возвращаем заявку

    def CancelOrder(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем

        if not self.orders.get(order.ref, False):  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем

        if order.ref not in self.orderNums:  # Если заявки нет в словаре заявок на бирже
            return  # то выходим, дальше не продолжаем

        orderNum = self.orderNums[order.ref]  # Номер заявки на бирже
        classCode, secCode = self.DataNameToClassSecCode(order.data._dataname)  # По названию тикера получаем код площадки и код тикера
        isStop = order.exectype in [Order.Stop, Order.StopLimit] and \
                 isinstance(self.qpProvider.GetOrderByNumber(orderNum)['data'], int)  # Задана стоп заявка и лимитная заявка не выставлена
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': classCode,  # Код площадки
            'SECCODE': secCode}  # Код тикера
        if isStop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'  # Будем удалять стоп заявку
            transaction['STOP_ORDER_KEY'] = str(orderNum)  # Номер стоп заявки на бирже
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'  # Будем удалять лимитную заявку
            transaction['ORDER_KEY'] = str(orderNum)  # Номер заявки на бирже
        self.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на рынок
        return order  # В список уведомлений ничего не добавляем. Ждем события OnTransReply

    def OCOCheck(self, order):
        """Проверка связанных заявок"""
        for orderRef, ocoRef in self.ocos.items():  # Пробегаемся по списку родительских заявок
            if ocoRef == order.ref:  # Если эта заявка для какой-то является родительской
                self.CancelOrder(self.orders[orderRef])  # то удаляем ту заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки есть родительская заявка
            ocoRef = self.ocos[order.ref]  # то получаем номер родительской заявки
            self.CancelOrder(self.orders[ocoRef])  # и удаляем родительскую заявку

    def OnConnected(self, data):
        dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на рынке
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Connected')
        self.isConnected = True  # QUIK подключен к серверу брокера

    def OnDisconnected(self, data):
        if not self.isConnected:  # Если QUIK отключен от сервера брокера
            return  # то не нужно дублировать сообщение, выходим, дальше не продолжаем
        dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на рынке
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, QUIK Disconnected')
        self.isConnected = False  # QUIK отключен от сервера брокера
