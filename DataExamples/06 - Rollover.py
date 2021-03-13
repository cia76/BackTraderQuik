from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Склейка фьючерсов (Rollover)
    symbols = ['SPBFUT.SiH9', 'SPBFUT.SiM9', 'SPBFUT.SiU9', 'SPBFUT.SiZ9',
    'SPBFUT.SiH0', 'SPBFUT.SiM0', 'SPBFUT.SiU0', 'SPBFUT.SiZ0',
    'SPBFUT.SiH1', 'SPBFUT.SiM1', 'SPBFUT.SiU1', 'SPBFUT.SiZ1']  # Тикеры для склейки
    store = QKStore(Host='192.168.1.7')
    data = [store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5) for symbol in symbols]  # Получаем по ним исторические данные
    cerebro.rolloverdata(name='Si', *data, checkdate=True, checkcondition=True)  # Склеенный тикер
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
