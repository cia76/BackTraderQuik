from datetime import datetime
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
# from BackTraderQuik.QKBroker import QKBroker  # Брокер QUIK для вызвова напрямую (не рекомендуется)


class LiveTradingEvents(bt.Strategy):
    """Отображает:
    - Статус приходящих баров (DELAYED / CONNECTED / DISCONNECTED / LIVE)
    - При приходе нового бара цены/объем. В статусе LIVE свободные средства / баланс счета
    - Статус перехода к новым барам
    - Изменение статуса заявок
    - Изменение статуса позиции
    """
    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if dt is None else dt # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные

    def next(self):
        """Получение следующего исторического/нового бара"""
        for data in self.datas:  # Пробегаемся по всем запрошенным барам
            self.log(f'{data.p.dataname} Open={data.open[0]:.2f}, High={data.high[0]:.2f}, Low={data.low[0]:.2f}, Close={data.close[0]:.2f}, Volume={data.volume[0]:.0f}')
        if self.isLive:
            self.log(f'Свободные средства: {self.broker.getcash()}, Баланс: {self.broker.getvalue()}')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        dataStatus = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(dataStatus)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = dataStatus == 'LIVE'

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in [order.Submitted, order.Accepted]:  # Если заявка не исполнена (отправлена брокеру или принята брокером)
            return  # то статус заявки не изменился, выходим, дальше не продолжаем

        if order.status in [order.Completed]:  # Если заявка исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:  # Заявка отменена, нет средств, отклонена брокером
            self.log('Canceled/Margin/Rejected')

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if not trade.isclosed:  # Если позиция не закрыта
            return  # то статус позиции не изменился, выходим, дальше не продолжаем

        self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader

    symbol = 'SPBFUT.SiH1'

    # clientCode = '<Ваш код клиента>'  # Код клиента (присваивается брокером)
    # symbol = 'TQBR.GAZP'

    cerebro.addstrategy(LiveTradingEvents)  # Добавляем торговую систему
    # broker = QKBroker(Host='192.168.1.7')  # Можно вызывать данные напрямую (не рекомендуется)
    store = QKStore(Host='192.168.1.7')  # Хранилище QUIK
    broker = store.getbroker()  # Брокер со счетом по умолчанию (срочный рынок РФ)
    # broker = store.getbroker(ClientCode=clientCode, FirmId='MC0063100000', TradeAccountId='L01-00000F00', LimitKind=2, CurrencyCode='SUR')  # Брокер со счетом фондового рынка РФ
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2021, 2, 15, 10, 00), LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.run()  # Запуск торговой системы
