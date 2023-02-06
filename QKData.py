from datetime import datetime, timedelta, time

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from BackTraderQuik import QKStore


class MetaQKData(AbstractDataBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaQKData, cls).__init__(name, bases, dct)  # Инициализируем класс данных
        QKStore.DataCls = cls  # Регистрируем класс данных в хранилище QUIK


class QKData(with_metaclass(MetaQKData, AbstractDataBase)):
    """Данные QUIK"""
    params = (
        ('FourPriceDoji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('LiveBars', False),  # False - только история, True - история и новые бары
    )

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.LiveBars

    def __init__(self, **kwargs):
        self.interval = self.p.compression  # Для минутных временнЫх интервалов ставим кол-во минут
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал
            self.interval = 1440  # В минутах
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            self.interval = 10080  # В минутах
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            self.interval = 23200  # В минутах

        self.store = QKStore(**kwargs)  # Передаем параметры в хранилище QUIK. Может работать самостоятельно, не через хранилище
        self.classCode, self.secCode = self.store.data_name_to_class_sec_code(self.p.dataname)  # По тикеру получаем код площадки и код тикера

        self.jsonBars = []  # Исторические бары после применения фильтров
        self.newCandleSubscribed = False  # Наличие подписки на получение новых баров
        self.liveMode = False  # Режим. False = Получение истории, True = Получение новых баров

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super(QKData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        super(QKData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        json_bars = self.store.qpProvider.GetCandlesFromDataSource(self.classCode, self.secCode, self.interval, 0)['data']  # Получаем все бары из QUIK
        for bar in json_bars:  # Пробегаемся по всем полученным барам из QUIK
            if self.is_bar_valid(bar, False):  # Если исторический бар соответствует всем условиям выборки
                self.jsonBars.append(bar)  # то добавляем бар
        if len(self.jsonBars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических баров

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        if not self.newCandleSubscribed:  # Если получаем исторические данные
            if len(self.jsonBars) > 0:  # Если есть исторические данные
                bar = self.jsonBars[0]  # Берем первый бар из выборки, с ним будем работать
                self.jsonBars.remove(bar)  # Убираем его из хранилища новых баров
            else:  # Если исторических данных нет
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                if not self.p.LiveBars:  # Если новые бары не принимаем
                    return False  # Больше сюда заходить не будем
                if not self.store.qpProvider.IsSubscribed(self.classCode, self.secCode, self.interval)['data']:  # Если не было подписки на тикер/интервал
                    self.store.qpProvider.SubscribeToCandles(self.classCode, self.secCode, self.interval)  # Подписываемся на новые бары
                    self.store.subscribedSymbols.append({'class': self.classCode, 'sec': self.secCode, 'interval': self.interval})  # Добавляем в список подписанных тикеров/интервалов
                self.newCandleSubscribed = True  # Дальше будем получать новые бары по подписке
                return None  # Будем заходить еще
        else:  # Если получаем новые бары по подписке
            if len(self.store.newBars) == 0:  # Если в хранилище никаких новых баров нет
                return None  # то нового бара нет, будем заходить еще
            new_bars = [newBar for newBar in self.store.newBars  # Смотрим в хранилище новых баров
                        if newBar['class'] == self.classCode and  # бары с нужным кодом площадки,
                        newBar['sec'] == self.secCode and  # тикером,
                        int(newBar['interval']) == self.interval]  # и интервалом
            if len(new_bars) == 0:  # Если новый бар еще не появился
                return None  # то нового бара нет, будем заходить еще
            bar = new_bars[0]  # Получаем текущий (первый) бар из выборки, с ним будем работать
            self.store.newBars.remove(bar)  # Убираем его из хранилища новых баров
            if not self.is_bar_valid(bar, True):  # Если бар по подписке не соответствует всем условиям выборки
                return None  # то нового бара нет, будем заходить еще
            dt_open = self.get_bar_open_date_time(bar)  # Дата/время открытия бара
            dt_next_bar_close = self.get_bar_close_date_time(dt_open, 2)  # Биржевое время закрытия следующего бара
            time_market_now = self.get_quik_date_time_now()  # Текущее биржевое время из QUIK
            # Переходим в режим получения новых баров (LIVE), если не находимся в этом режиме и
            # следующий бар закроется в будущем (т.к. пришедший бар закрылся в прошлом), или пришел последний бар предыдущей сессии
            if not self.liveMode and (dt_next_bar_close > time_market_now or dt_open.day != time_market_now.day):
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых баров
                self.liveMode = True  # Переходим в режим получения новых баров (LIVE)
            # Бывает ситуация, когда QUIK несколько минут не передает новые бары. Затем передает все пропущенные
            # Чтобы не совершать сделки на истории, меняем режим торгов на историю до прихода нового бара
            elif self.liveMode and dt_next_bar_close <= time_market_now:  # Если в режиме получения новых баров, и следующий бар закроется до текущего времени на бирже
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
                self.liveMode = False  # Переходим в режим получения истории
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(self.get_bar_open_date_time(bar))  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['open'])  # Open
        self.lines.high[0] = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['high'])  # High
        self.lines.low[0] = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['low'])  # Low
        self.lines.close[0] = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['close'])  # Close
        self.lines.volume[0] = bar['volume']  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в QUIK не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(QKData, self).stop()
        if self.newCandleSubscribed:  # Если принимали новые бары и подписались на них
            self.store.qpProvider.UnsubscribeFromCandles(self.classCode, self.secCode, self.interval)  # Отменяем подписку на новые бары
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Функции

    def is_bar_valid(self, bar, live):
        """Проверка бара на соответствие условиям выборки"""
        dt_open = self.get_bar_open_date_time(bar)  # Дата/время открытия бара
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата/время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        high = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['high'])  # High
        low = self.store.quik_to_bt_price(self.classCode, self.secCode, bar['low'])  # Low
        if not self.p.FourPriceDoji and high == low:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_quik_date_time_now()  # Текущее биржевое время
        if not live:  # Если получаем исторические данные
            if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
                return False  # то бар не соответствует условиям выборки
        else:  # Если получаем новые бары по подписке
            if date2num(dt_open) <= self.lines.datetime[-1]:  # Если получили предыдущий или более старый бар
                return False  # то выходим, дальше не продолжаем
            time_market_now += timedelta(seconds=3)  # Текущее биржевое время из QUIK. Корректируем его на несколько секунд, т.к. минутный бар может прийти в 59 секунд прошлой минуты
            if dt_close > time_market_now:  # Если получили несформированный бар. Например, дневной бар в середине сессии
                return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    @staticmethod
    def get_bar_open_date_time(bar):
        """Дата/время открытия бара"""
        dt_json = bar['datetime']  # Получаем составное значение даты и времени открытия бара
        return datetime(dt_json['year'], dt_json['month'], dt_json['day'], dt_json['hour'], dt_json['min'])  # Время открытия бара

    def get_bar_close_date_time(self, dt_open, period=1):
        """Дата/время закрытия бара"""
        return dt_open + timedelta(minutes=self.interval * period)  # Время закрытия бара

    def get_quik_date_time_now(self):
        """Текущая дата и время из QUIK (МСК)"""
        if not self.liveMode:  # Если не находимся в режиме получения новых баров
            return datetime.now(self.store.MarketTimeZone).replace(tzinfo=None)  # То время МСК получаем из локального времени
        d = self.store.qpProvider.GetInfoParam('TRADEDATE')['data']  # Дата на сервере в виде строки dd.mm.yyyy
        t = self.store.qpProvider.GetInfoParam('SERVERTIME')['data']  # Время на сервере в виде строки hh:mi:ss
        return datetime.strptime(f'{d} {t}', '%d.%m.%Y %H:%M:%S')  # Переводим строки в дату и время и возвращаем их
