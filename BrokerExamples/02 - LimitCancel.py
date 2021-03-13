from datetime import datetime
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class LimitCancel(bt.Strategy):
    """Выставляем заявку на покупку на 1% ниже цены закрытия
    Если за 1 бар заявка не срабатывает, то закрываем ее
    Если срабатывает, то закрываем позицию. Неважно, с прибылью или убытком
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на 1% ниже цены закрытия
    )
    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if dt is None else dt # Заданная дата или дата текущего бара
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

        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то выходим, дальше не продолжаем

        if not self.position:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем ее
            limitPrice = self.close[0] * (100 - self.params.LimitPct) / 100  # На 1% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=limitPrice)  # Лимитная заявка на покупку
        else:  # Если позиция есть
            self.order = self.sell(size=self.position.size)  # Заявка на продажу всей позиции по рыночной цене

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        dataStatus = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(dataStatus)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = dataStatus == 'LIVE'

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in [order.Submitted, order.Accepted]:  # Если заявка не исполнена (отправлена брокеру или принята брокером)
            self.log(f'Order Status: {order.getstatusname()}. TransId={order.ref}')
            return  # то выходим, дальше не продолжаем

        if order.status in [order.Canceled]:  # Если заявка отменена
            self.log(f'Order Status: {order.getstatusname()}. TransId={order.ref}')
            return  # то выходим, дальше не продолжаем

        if order.status in [order.Completed]:  # Если заявка исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
        elif order.status in [order.Margin, order.Rejected]:  # Нет средств, или заявка отклонена брокером
            self.log(f'Order Status: {order.getstatusname()}. TransId={order.ref}')
        self.order = None  # Этой заявки больше нет

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if not trade.isclosed:  # Если позиция не закрыта
            return  # то статус позиции не изменился, выходим, дальше не продолжаем

        self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader

    clientCode = 'D61904'  # Код клиента (присваивается брокером)
    symbol = 'SPBFUT.SiH1'
    # symbol = 'TQBR.GAZP'

    cerebro.addstrategy(LimitCancel, LimitPct=1)  # Добавляем торговую систему с параметрами
    store = QKStore(Host='192.168.1.7')  # Хранилище QUIK
    broker = store.getbroker(use_positions=False)  # Брокер со счетом по умолчанию (срочный рынок РФ)
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2021, 2, 15, 10, 00),
                         LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)  # Кол-во акций для покупки/продажи
    cerebro.run()  # Запуск торговой системы
