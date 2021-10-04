import backtrader as bt

class PrintStatusAndBars(bt.Strategy):
    """
    - Отображает статус подключения
    - При приходе нового бара отображает его цены/объем
    - Отображает статус перехода к новым барам
    """
    params = (  # Параметры торговой системы
        ('name', ''),  # Название торговой системы
        ('symbols', ''),  # Список торгуемых тикеров. По умолчанию торгуем все тикеры
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if dt is None else dt  # Заданная дата или дата последнего бара первого тикера ТС
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные

    def next(self):
        """
        Приход нового бара тикера
        """
        if self.p.name != '':  # Если указали название торговой системы, то будем ждать прихода всех баров
            lastdatetimes = [bt.num2date(data.datetime[0]) for data in self.datas]  # Дата и время последнего бара каждого тикера
            if lastdatetimes.count(lastdatetimes[0]) != len(lastdatetimes):  # Если дата и время последних баров не идентичны
                return  # то еще не пришли все новые бары. Ждем дальше, выходим
            print(self.p.name)
        for data in self.datas:  # Пробегаемся по всем запрошенным тикерам
            if self.p.symbols == '' or data.p.dataname in self.p.symbols:  # Если торгуем все тикеры или данный тикер
                self.log(f'{data.p.dataname} Open={data.open[0]:.2f}, High={data.high[0]:.2f}, Low={data.low[0]:.2f}, Close={data.close[0]:.2f}, Volume={data.volume[0]:.0f}',
                     bt.num2date(data.datetime[0]))

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статсуса приходящих баров"""
        dataStatus = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(f'{data.p.dataname} - {dataStatus}')  # Статус приходит для каждого тикера отдельно
        self.isLive = dataStatus == 'LIVE'  # В Live режим переходим после перехода первого тикера
