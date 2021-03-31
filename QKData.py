from datetime import datetime, timedelta
import pytz

from backtrader.feed import AbstractDataBase
from backtrader import TimeFrame, date2num
from backtrader.utils.py3 import with_metaclass

from BackTraderQuik import QKStore


class MetaQKData(AbstractDataBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaQKData, cls).__init__(name, bases, dct)  # Инициализируем класс данных
        QKStore.DataCls = cls  # Регистрируем класс данных в хранилище QUIK


class QKData(with_metaclass(MetaQKData, AbstractDataBase)):
    """Данные QUIK"""
    params = (
        ('LiveBars', False),)  # Только исторические данные

    def islive(self):
        """Если подаем новые бары, то ``Cerebro`` не будет запускать ``preload`` и ``runonce``, т.к. новые бары должны идти один за другим"""
        return self.p.LiveBars

    def __init__(self, **kwargs):
        self.interval = self.p.compression  # Для минутных временнЫх интервалов ставим кол-во минут
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал
            self.interval = 1440
        elif self.p.timeframe == TimeFrame.Days:  # Недельный временной интервал
            self.interval = 10080
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            self.interval = 23200

        self.store = QKStore(**kwargs)  # Передаем параметры в хранилище QUIK. Может работать самостоятельно, не через хранилище
        self.classCode, self.secCode = self.store.DataNameToClassSecCode(self.p.dataname)  # По тикеру получаем код площадки и код тикера

        self.jsonBars = None  # Все исторические бары
        self.lastBarId = 0  # Последний номер бара (последний бар может быть еще несформирован)
        self.jsonBar = None  # Текущий бар
        self.barId = 0  # Начинаем загрузку баров в BackTrader с начала (нулевого бара)
        self.newCandleSubscribed = False  # Наличие подписки на получение новых баров
        self.lifeMode = False  # Режим. False = Получение истории, True = Получение новых баров

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super(QKData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        super(QKData, self).start()
        if self.p.tz is None:  # Если временнАя зона не указана
            self.p.tz = QKStore.MarketTimeZone  # то берем московское время биржи
            # HACK Хоть мы и задаем временнУю зону биржи, но параметры fromdate и todate переводятся в GMT
            # Поэтому, считаем, что время задается в GMT, переводим его во время биржи и удаляем временнУю зону
            if self.p.fromdate is not None:  # Если задана дата начала получения исторических данных
                dt = self.p.fromdate = pytz.utc.localize(self.p.fromdate).astimezone(self.p.tz)
                self.p.fromdate = dt.replace(tzinfo=None)
            if self.p.todate is not None:  # Если задана дата окончания получения исторических данных
                dt = self.p.todate = pytz.utc.localize(self.p.todate).astimezone(self.p.tz)
                self.p.todate = dt.replace(tzinfo=None)
        if self.p.sessionstart is None:  # Если время начала сессии не указано
            self.p.sessionstart = datetime.time(10, 00)  # то берем время начала сессии на бирже
        if self.p.sessionend is None:  # Если время окончания сессии не указано
            self.p.sessionend = datetime.time(23, 50)  # то берем время окончания сессии на бирже
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        self.jsonBars = self.store.qpProvider.GetCandlesFromDataSource(self.classCode, self.secCode, self.interval, 0)['data']  # Получаем все бары из QUIK
        barsCount = len(self.jsonBars)  # Кол-во полученных баров
        if barsCount == 0:  # Если бары не получены
            self.put_notification(self.DISCONNECTED)  # то отправляем уведомление о невозможности отправки исторических баров
            return  # выходим, дальше не продолжаем

        self.put_notification(self.CONNECTED)  # Отправляем уведомление об успешном подключении
        self.lastBarId = barsCount - 1  # Последний номер бара
        jsonDateTime = self.jsonBars[self.lastBarId]['datetime']  # Вытаскиваем составное значение даты и времени открытия бара
        dt = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара
        timeOpen = self.p.tz.localize(dt)  # Биржевое время открытия бара
        timeClose = timeOpen + timedelta(minutes=self.interval)  # Биржевое время закрытия бара
        timeMarketNow = datetime.now(self.p.tz)  # Текущее биржевое время
        if timeClose > timeMarketNow and timeMarketNow.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            self.lastBarId -= 1  # то последний бар из истории не принимаем

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        if self.newCandleSubscribed:  # Если получаем новые бары по подписке
            if self.jsonBar is None:  # Если новый бар еще не появился
                return None  # то нового бара нет, будем заходить еще
        else:  # Если получаем исторические данные
            if len(self.jsonBars) == 0:  # Если исторических данных нет (QUIK отключен от сервера брокера)
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                return False  # Больше сюда заходить не будем
            if self.barId > self.lastBarId:  # Если получили все бары из истории
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                if not self.p.LiveBars:  # Если новые бары не принимаем
                    return False  # Больше сюда заходить не будем
                # Принимаем новые бары
                self.jsonBar = None  # Сбрасываем последний бар истории, чтобы он не дублировался как новый бар
                self.store.qpProvider.OnNewCandle = self.OnNewCandle  # Получение нового бара. В первый раз получим все бары с начала прошлой сессии
                self.store.qpProvider.SubscribeToCandles(self.classCode, self.secCode, self.interval)  # Подписываемся на новые бары
                self.newCandleSubscribed = True  # Получаем новые бары по подписке
                return None  # Будем заходить еще
            else:  # Если еще не получили все бары из истории
                self.jsonBar = self.jsonBars[self.barId]  # Получаем следующий бар из истории

        # Исторический / новый бар
        jsonDateTime = self.jsonBar['datetime']  # Вытаскиваем составное значение даты и времени открытия бара
        dt = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Время открытия бара
        self.lines.datetime[0] = date2num(dt)  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['open'])
        self.lines.high[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['high'])
        self.lines.low[0] = self.store.QKToBTPrice(self.classCode, self.secCode, self.jsonBar['low'])
        self.lines.close[0] = self.store.QKToBTPrice(self.classCode, self.secCode,  self.jsonBar['close'])
        self.lines.volume[0] = self.jsonBar['volume']
        self.lines.openinterest[0] = 0  # Открытый интерес в QUIK не учитывается

        # Исторический бар
        if self.barId <= self.lastBarId:  # Если еще не получили все бары из истории
            self.barId += 1  # то переходим на следующий бар
            return True  # Будем заходить сюда еще

        # Новый бар
        timeOpen = self.p.tz.localize(dt)  # Биржевое время открытия бара
        timeNextClose = timeOpen + timedelta(minutes=self.interval*2)  # Биржевое время закрытия следующего бара
        timeMarketNow = datetime.now(self.p.tz)  # Текущее биржевое время
        if not self.lifeMode and timeNextClose > timeMarketNow:  # Если не в режиме получения новых баров, и следующий бар закроется позже текущего времени на бирже
            self.put_notification(self.LIVE)  # Уведомляем о получении новых баров
            self.lifeMode = True  # Переходим в режим получения новых баров
        # Бывает ситуация, когда QUIK несколько минут не передает новые бары. Затем передает все пропущенные
        # Чтобы не совершать сделки на истории, меняем режим торгов на историю до прихода нового бара
        elif self.lifeMode and timeNextClose <= timeMarketNow:  # Если в режиме получения новых баров, и следующий бар закроется до текущего времени на бирже
            self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
            self.lifeMode = False  # Переходим в режим получения истории

        self.jsonBar = None  # Сбрасываем текущий бар
        return True  # Будем заходить еще

    def stop(self):
        super(QKData, self).stop()
        if self.newCandleSubscribed:  # Если принимали новые бары и подписались на них
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
            self.store.qpProvider.UnsubscribeFromCandles(self.classCode, self.secCode, self.interval)  # Отменяем подписку на новые бары
            self.store.qpProvider.OnNewCandle = self.store.qpProvider.DefaultHandler  # Возвращаем обработчик по умолчанию
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    def OnNewCandle(self, data):
        """Обработчик события прихода нового бара"""
        self.jsonBar = None  # Сбрасываем текущий бар
        jsonData = data['data']  # Новый бар
        if jsonData['class'] != self.classCode or jsonData['sec'] != self.secCode or int(jsonData['interval'] != self.interval):  # Если бар по другому тикеру / временнОму интервалу
            return  # то выходим, дальше не продолжаем
        jsonDateTime = jsonData['datetime']  # Вытаскиваем составное значение даты и времени начала бара
        dt = datetime(jsonDateTime['year'], jsonDateTime['month'], jsonDateTime['day'], jsonDateTime['hour'], jsonDateTime['min'])  # Переводим в формат datetime
        if date2num(dt) <= self.lines.datetime[-1]:  # Если получили предыдущий или более старый бар
            return   # то выходим, дальше не продолжаем
        self.jsonBar = jsonData  # Новый бар получен
