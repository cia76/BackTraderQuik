from datetime import datetime, time
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class Brackets(bt.Strategy):
    """
    Выставляем родительскую заявку на покупку на 1% ниже цены закрытия
    Вместе с ней выставляем дочерние заявки на выход с убытком/прибылью
    При исполнении родительской заявки выставляем все дочерние
    При исполнении дочерней заявки отменяем все остальные неисполненные дочерние
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на 1% ниже цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if dt is None else dt  # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия
        self.isLive = False  # Сначала будут приходить исторические данные
        self.order = None  # Заявка

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:
            return

        if self.order and self.order.status == self.order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем

        if not self.position:  # Если позиции нет
            if self.order:  # А заявка на вход есть (не исполнена)
                self.cancel(self.order)  # то снимаем заявку на вход
            limitPrice = self.close[0] * (1 - self.p.LimitPct / 100)  # Цена входа на 1% ниже цены закрытия
            stopPrice = self.close[0] * (1 - self.p.LimitPct / 100 * 2)  # Цена выхода с убытком на 2% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=limitPrice, transmit=False)  # Родительская лимитная заявка на покупку
            self.sell(exectype=bt.Order.Stop, price=stopPrice, parent=self.order, transmit=False)  # Дочерняя стоп заявка на продажу с убытком 1%
            self.sell(exectype=bt.Order.Limit, price=self.close[0], parent=self.order, transmit=True)  # Дочерняя лимитная заявка на продажу с прибылью 1%

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        dataStatus = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(dataStatus)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = dataStatus == 'LIVE'

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (order.Created, order.Submitted, order.Accepted):  # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
            self.log(f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status in (order.Canceled, order.Margin, order.Rejected, order.Expired):  # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
            self.log(f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == order.Partial:  # Если заявка частично исполнена
            self.log(f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Этой заявки больше нет

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader

    clientCode = '<Ваш код клиента>'  # Код клиента (присваивается брокером)
    firmId = '<Код фирмы>'  # Код фирмы (присваивается брокером)
    # symbol = 'TQBR.GAZP'
    symbol = 'SPBFUT.SiH2'

    cerebro.addstrategy(Brackets, LimitPct=1)  # Добавляем торговую систему с параметрами
    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # store = QKStore(Host='<Ваш IP адрес>')  # Хранилище QUIK (QUIK на удаленном компьютере)
    broker = store.getbroker(use_positions=False)  # Брокер со счетом по умолчанию (срочный рынок РФ)
    # broker = store.getbroker(use_positions=False, ClientCode=clientCode, FirmId=firmId, TradeAccountId='L01-00000F00', LimitKind=2, CurrencyCode='SUR', IsFutures=False)  # Брокер со счетом фондового рынка РФ

    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2022, 2, 15), sessionstart=time(7, 0), LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)  # Кол-во акций для покупки/продажи
    cerebro.run()  # Запуск торговой системы
