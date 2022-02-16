from datetime import datetime, time
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class Brackets(bt.Strategy):
    """
    Выставляем родительскую заявку на покупку на n% ниже цены закрытия
    Вместе с ней выставляем дочерние заявки на выход с n% убытком/прибылью
    При исполнении родительской заявки выставляем все дочерние
    При исполнении дочерней заявки отменяем все остальные неисполненные дочерние
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if dt is None else dt  # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход в позицию

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем заявку на вход
            closeMinusN = self.data.close[0] * (1 - self.p.LimitPct / 100)  # Цена на n% ниже цены закрытия
            closeMinus2N = self.data.close[0] * (1 - self.p.LimitPct / 100 * 2)  # Цена на 2n% ниже цены закрытия
            # self.order = self.buy(exectype=bt.Order.Limit, price=closeMinusN, transmit=False)  # Родительская лимитная заявка на покупку
            # orderStop = self.sell(exectype=bt.Order.Stop, price=closeMinus2N, size=self.order.size, parent=self.order, transmit=False)  # Дочерняя стоп заявка на продажу с убытком n%
            # orderLimit = self.sell(exectype=bt.Order.Limit, price=self.close[0], size=self.order.size, parent=self.order, transmit=True)  # Дочерняя лимитная заявка на продажу с прибылью n%
            self.order, orderStop, orderLimit = self.buy_bracket(limitprice=self.data.close[0], price=closeMinusN, stopprice=closeMinus2N)  # Bracket заявка в BT

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        dataStatus = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(dataStatus)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = dataStatus == 'LIVE'  # Режим реальной торговли

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):  # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
            self.log(f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status in (bt.Order.Canceled, bt.Order.Margin, bt.Order.Rejected, bt.Order.Expired):  # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
            self.log(f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Partial:  # Если заявка частично исполнена
            self.log(f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

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

    cerebro.addstrategy(Brackets, LimitPct=1)  # Добавляем торговую систему с лимитным входом в n%
    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # store = QKStore(Host='<Ваш IP адрес>')  # Хранилище QUIK (К QUIK на удаленном компьютере обращаемся по IP или названию)
    broker = store.getbroker(use_positions=False)  # Брокер со счетом по умолчанию (срочный рынок РФ)
    # broker = store.getbroker(use_positions=False, ClientCode=clientCode, FirmId=firmId, TradeAccountId='L01-00000F00', LimitKind=2, CurrencyCode='SUR', IsFutures=False)  # Брокер со счетом фондового рынка РФ
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2022, 2, 16), sessionstart=time(7, 0), LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)  # Кол-во акций для покупки/продажи
    cerebro.run()  # Запуск торговой системы
