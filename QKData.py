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
        self.classCode, self.secCode = self.store.DataNameToClassSecCode(self.p.dataname)  # По тикеру получаем код площадки и код тикера

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
        jsonBars = self.store.qpProvider.GetCandlesFromDataSource(self.classCode, self.secCode, self.interval, 0)['data']  # Получаем все бары из QUIK
        for bar in jsonBars:  # Пробегаемся по всем полученным барам из QUIK
            if self.IsBarValid(bar, False):  # Если исторический бар соответствует всем условиям выборки
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
            newBars = [newBar for newBar in self.store.newBars  # Смотрим в хранилище новых баров
                       if newBar['class'] == self.classCode and  # бары с нужным кодом площадки,
                       newBar['sec'] == self.secCode and  # тикером,
                       int(newBar['interval']) == self.interval]  # и интервалом
            if len(newBars) == 0:  # Если новый бар еще не появился
                return None  # то нового бара нет, будем заходить еще
            bar = newBars[0]  # Получаем текущий (первый) бар из выборки, с ним будем работать
            self.store.newBars.remove(bar)  # Убираем его из хранилища новых баров
            if not self.IsBarValid(bar, True):  # Если бар по подписке не соответствует всем условиям выборки
                return None  # то нового бара нет, будем заходить еще
            dtOpen = self.GetBarOpenDateTime(bar)  # Дата/время открытия бара
            dtNextBarClose = self.GetBarCloseDateTime(dtOpen, 2)  # Биржевое время закрытия следующего бара
            timeMarketNow = self.GetQUIKDateTimeNow()  # Текущее биржевое время из QUIK
            # Переходим в режим получения новых баров (LIVE), если не находимся в этом режиме и
            # следующий бар закроется в будущем (т.к. пришедший бар закрылся в прошлом), или пришел последний бар предыдущей сессии
            if not self.liveMode and (dtNextBarClose > timeMarketNow or dtOpen.day != timeMarketNow.day):
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых баров
                self.liveMode = True  # Переходим в режим получения новых баров (LIVE)
            # Бывает ситуация, когда QUIK несколько минут не передает новые бары. Затем передает все пропущенные
            # Чтобы не совершать сделки на истории, меняем режим торгов на историю до прихода нового бара
            elif self.liveMode and dtNextBarClose <= timeMarketNow:  # Если в режиме получения новых баров, и следующий бар закроется до текущего времени на бирже
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
                self.liveMode = False  # Переходим в режим получения истории

        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(self.GetBarOpenDateTime(bar))  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = self.store.QKToBTPrice(self.classCode, self.secCode, bar['open'])  # Open
        self.lines.high[0] = self.store.QKToBTPrice(self.classCode, self.secCode, bar['high'])  # High
        self.lines.low[0] = self.store.QKToBTPrice(self.classCode, self.secCode, bar['low'])  # Low
        self.lines.close[0] = self.store.QKToBTPrice(self.classCode, self.secCode,  bar['close'])  # Close
        self.lines.volume[0] = bar['volume']  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в QUIK не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(QKData, self).stop()
        if self.newCandleSubscribed:  # Если принимали новые бары и подписались на них
            self.store.qpProvider.UnsubscribeFromCandles(self.classCode, self.secCode, self.interval)  # Отменяем подписку на новые бары
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    def IsBarValid(self, bar, live):
        """Проверка бара на соответствие условиям выборки"""
        dtOpen = self.GetBarOpenDateTime(bar)  # Дата/время открытия бара
        if self.p.sessionstart != time.min and dtOpen.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dtClose = self.GetBarCloseDateTime(dtOpen)  # Дата/время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dtClose.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        h = self.store.QKToBTPrice(self.classCode, self.secCode, bar['high'])  # High
        l = self.store.QKToBTPrice(self.classCode, self.secCode, bar['low'])  # Low
        if not self.p.FourPriceDoji and h == l:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        timeMarketNow = self.GetQUIKDateTimeNow()  # Текущее биржевое время
        if not live:  # Если получаем исторические данные
            if dtClose > timeMarketNow and timeMarketNow.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
                return False  # то бар не соответствует условиям выборки
        else:  # Если получаем новые бары по подписке
            if date2num(dtOpen) <= self.lines.datetime[-1]:  # Если получили предыдущий или более старый бар
                return False  # то выходим, дальше не продолжаем
            timeMarketNow += timedelta(seconds=3)  # Текущее биржевое время из QUIK. Корректируем его на несколько секунд, т.к. минутный бар может прийти в 59 секунд прошлой минуты
            if dtClose > timeMarketNow:  # Если получили несформированный бар. Например, дневной бар в середине сессии
                return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    def GetBarOpenDateTime(self, bar):
        """Дата/время открытия бара"""
        jsonDateTime = bar['datetime']  # Получаем составное значение даты и времени открытия бара
        return datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара

    def GetBarCloseDateTime(self, dtOpen, period=1):
        """Дата/время закрытия бара"""
        return dtOpen + timedelta(minutes=self.interval*period)  # Время закрытия бара

    def GetQUIKDateTimeNow(self):
        """Текущая дата и время МСК"""
        if not self.liveMode:  # Если не находимся в режиме получения новых баров
            return datetime.now(self.store.MarketTimeZone).replace(tzinfo=None)  # То время МСК получаем из локального времени
        d = self.store.qpProvider.GetInfoParam('TRADEDATE')['data']  # Дата на сервере в виде строки dd.mm.yyyy
        t = self.store.qpProvider.GetInfoParam('SERVERTIME')['data']  # Время на сервере в виде строки hh:mi:ss
        return datetime.strptime(f'{d} {t}', '%d.%m.%Y %H:%M:%S')  # Переводим строки в дату и время и возвращаем их
