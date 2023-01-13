from datetime import datetime, timedelta, time
import pytz

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
        self.jsonBar = None  # Текущий бар
        self.barId = 0  # Начинаем загрузку баров в BackTrader с начала (нулевого бара)
        self.newCandleSubscribed = False  # Наличие подписки на получение новых баров
        self.liveMode = False  # Режим. False = Получение истории, True = Получение новых баров

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super(QKData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        super(QKData, self).start()
        if self.p.tz is None:  # Если временнАя зона не указана
            self.p.tz = self.store.MarketTimeZone  # то берем московское время биржи
        # HACK В BackTrader параметры fromdate и todate нужно задавать в GMT. Далее они будут переведены во время биржи
        # Если хотим задавать интервал не в GMT, а по времени работы биржи, то нужно выполнить преобразования:
        if self.p.fromdate is not None:  # Если задана дата начала получения исторических данных
            dt = self.p.fromdate = pytz.utc.localize(self.p.fromdate).astimezone(self.p.tz)  # Время берем в GMT, переводим его во время биржи
            self.p.fromdate = dt.replace(tzinfo=None)  # и удаляем временнУю зону
        if self.p.todate is not None:  # Если задана дата окончания получения исторических данных
            dt = self.p.todate = pytz.utc.localize(self.p.todate).astimezone(self.p.tz)  # Время берем в GMT, переводим его во время биржи
            self.p.todate = dt.replace(tzinfo=None)  # и удаляем временнУю зону
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
            if len(self.jsonBars) == 0:  # Если исторических данных нет
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                return False  # Больше сюда заходить не будем
            if self.barId < len(self.jsonBars):  # Если еще не получили все бары из истории
                self.jsonBar = self.jsonBars[self.barId]  # Получаем текущий бар из истории
                self.barId += 1  # Перемещаем указатель на следующий бар
            else:  # Если получили все бары из истории
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                if not self.p.LiveBars:  # Если новые бары не принимаем
                    return False  # Больше сюда заходить не будем
                if not self.store.qpProvider.IsSubscribed(self.classCode, self.secCode, self.interval)['data']:  # Если не было подписки на тикер/интервал
                    self.store.qpProvider.SubscribeToCandles(self.classCode, self.secCode, self.interval)  # Подписываемся на новые бары
                    self.store.subscribedSymbols.append({'class': self.classCode, 'sec': self.secCode, 'interval': self.interval})  # Добавляем в список подписанных тикеров/интервалов
                    print(f'Подписка {self.classCode}.{self.secCode} на интервале {self.interval}')
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
            self.jsonBar = newBars[0]  # Получаем текущий (первый) бар из выборки, с ним будем работать
            self.store.newBars.remove(self.jsonBar)  # Убираем его из хранилища новых баров
            if not self.IsBarValid(self.jsonBar, True):  # Если бар по подписке не соответствует всем условиям выборки
                return None  # то нового бара нет, будем заходить еще
            jsonDateTime = self.jsonBar['datetime']  # Вытаскиваем составное значение даты и времени открытия бара
            dtOpen = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара
            dtNextBarClose = dtOpen + timedelta(minutes=self.interval * 2)  # Биржевое время закрытия следующего бара
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
        jsonDateTime = self.jsonBar['datetime']  # Вытаскиваем составное значение даты и времени открытия бара
        dtOpen = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара
        self.lines.datetime[0] = date2num(dtOpen)  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['open'])  # Open
        self.lines.high[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['high'])  # High
        self.lines.low[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['low'])  # Low
        self.lines.close[0] = self.store.QKToBTPrice(self.classCode, self.secCode,  self.jsonBar['close'])  # Close
        self.lines.volume[0] = self.jsonBar['volume']  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в QUIK не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(QKData, self).stop()
        if self.newCandleSubscribed:  # Если принимали новые бары и подписались на них
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
            self.store.qpProvider.UnsubscribeFromCandles(self.classCode, self.secCode, self.interval)  # Отменяем подписку на новые бары
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    def IsBarValid(self, bar, live):
        """Проверка бара на соответствие условиям выборки"""
        jsonDateTime = bar['datetime']  # Вытаскиваем составное значение даты и времени открытия бара
        dtOpen = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара
        if self.p.sessionstart != time.min and dtOpen.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dtClose = dtOpen + timedelta(minutes=self.interval)  # Время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dtClose.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        h = self.store.QKToBTPrice(self.classCode, self.secCode, bar['high'])  # High
        l = self.store.QKToBTPrice(self.classCode, self.secCode, bar['low'])  # Low
        if not self.p.FourPriceDoji and h == l:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        if not live:  # Если получаем исторические данные
            timeClose = self.p.tz.localize(dtClose)  # Биржевое время закрытия бара
            timeMarketNow = datetime.now(self.p.tz)  # Текущее биржевое время из текущего локального времени
            if timeClose > timeMarketNow and timeMarketNow.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
                return False  # то бар не соответствует условиям выборки
        else:  # Если получаем новые бары по подписке
            timeMarketNow = self.GetQUIKDateTimeNow() + timedelta(seconds=3)  # Текущее биржевое время из QUIK. Корректируем его на несколько секунд, т.к. минутный бар может прийти в 59 секунд прошлой минуты
            if dtClose > timeMarketNow:  # Если получили несформированный бар. Например, дневной бар в середине сессии
                return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    def GetQUIKDateTimeNow(self):
        """Текущая дата и время МСК в QUIK"""
        d = self.store.qpProvider.GetInfoParam('TRADEDATE')['data']  # Дата на сервере в виде строки dd.mm.yyyy
        t = self.store.qpProvider.GetInfoParam('SERVERTIME')['data']  # Время на сервере в виде строки hh:mi:ss
        return datetime.strptime(f'{d} {t}', '%d.%m.%Y %H:%M:%S')  # Переводим строки в дату и время и возвращаем их

    def OnNewCandle(self, data):
        """Обработчик события прихода нового бара"""
        self.jsonBar = None  # Сбрасываем текущий бар
        jsonData = data['data']  # Новый бар
        if jsonData['class'] != self.classCode or jsonData['sec'] != self.secCode or int(jsonData['interval'] != self.interval):  # Если бар по другому тикеру / временнОму интервалу
            return  # то выходим, дальше не продолжаем
        jsonDateTime = jsonData['datetime']  # Вытаскиваем составное значение даты и времени начала бара
        dtOpen = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Переводим в формат datetime
        if date2num(dtOpen) <= self.lines.datetime[-1]:  # Если получили предыдущий или более старый бар
            return   # то выходим, дальше не продолжаем
        self.jsonBar = jsonData  # Новый бар получен
