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
        self.provider = QuikPy(host=self.p.Host, requests_port=self.p.RequestsPort, callbacks_port=self.p.CallbacksPort)  # Вызываем конструктор QuikPy с адресом хоста и портами
        self.symbols = {}  # Информация о тикерах
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из QUIK
        self.connected = True  # Считаем, что изначально QUIK подключен к серверу брокера
        self.class_codes = self.provider.GetClassesList()['data']  # Список классов. В некоторых таблицах тикер указывается без кода класса
        self.subscribed_symbols = []  # Список подписанных тикеров/интервалов

    def start(self):
        self.provider.OnConnected = self.on_connected  # Соединение терминала с сервером QUIK
        self.provider.OnDisconnected = self.on_disconnected  # Отключение терминала от сервера QUIK
        self.provider.OnNewCandle = lambda data: self.new_bars.append(data['data'])  # Обработчик новых баров по подписке из QUIK

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [notif for notif in iter(self.notifs.popleft, None)]

    def stop(self):
        self.provider.OnNewCandle = self.provider.DefaultHandler  # Возвращаем обработчик по умолчанию
        self.provider.CloseConnectionAndThread()  # Закрываем соединение для запросов и поток обработки функций обратного вызова

    # Функции

    def get_symbol_info(self, class_code, sec_code, reload=False):
        """Получение информации тикера

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param bool reload: Получить информацию из QUIK
        :return: Значение из кэша/QUIK или None, если тикер не найден
        """
        if reload or (class_code, sec_code) not in self.symbols:  # Если нужно получить информацию из QUIK или нет информации о тикере в справочнике
            symbol_info = self.provider.GetSecurityInfo(class_code, sec_code)  # Получаем информацию о тикере из QUIK
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
            class_code = self.provider.GetSecurityClass(self.class_codes, dataname)['data']  # Получаем код площадки по коду инструмента из имеющихся классов
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

    def on_connected(self, data):
        """Обработка событий подключения к QUIK"""
        dt = datetime.now(self.MarketTimeZone)  # Берем текущее время на бирже из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}: QUIK Подключен')
        self.connected = True  # QUIK подключен к серверу брокера
        print(f'Проверка подписки тикеров ({len(self.subscribed_symbols)})')
        for subscribed_symbol in self.subscribed_symbols:  # Пробегаемся по всем подписанным тикерам
            class_code = subscribed_symbol['class']  # Код площадки
            sec_code = subscribed_symbol['sec']  # Код тикера
            interval = subscribed_symbol['interval']  # Временной интервал
            print(f'{self.class_sec_code_to_data_name(class_code, sec_code)} на интервале {interval}', end=' ')
            if not self.provider.IsSubscribed(class_code, sec_code, interval)['data']:  # Если нет подписки на тикер/интервал
                self.provider.SubscribeToCandles(class_code, sec_code, interval)  # то переподписываемся
                print('нет подписки. Отправлен запрос на новую подписку')
            else:  # Если подписка была, то переподписываться не нужно
                print('есть подписка')

    def on_disconnected(self, data):
        """Обработка событий отключения от QUIK"""
        if not self.connected:  # Если QUIK отключен от сервера брокера
            return  # то не нужно дублировать сообщение, выходим, дальше не продолжаем
        dt = datetime.now(self.MarketTimeZone)  # Берем текущее время на бирже из локального
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}: QUIK Отключен')
        self.connected = False  # QUIK отключен от сервера брокера
