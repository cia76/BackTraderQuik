from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
# from BackTraderQuik.QKData import QKData  # Данные QUIK для вызвова напрямую (не рекомендуется)
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Один тикер, один временной интервал
    # symbol = 'TQBR.GAZP'
    symbol = 'SPBFUT.SiM1'
    # data = QKData(dataname=symbol, timeframe=TimeFrame.Days, Host='192.168.1.7')  # Можно вызывать данные напрямую (не рекомендуется)
    store = QKStore(Host='192.168.1.7')  # Хранилище QUIK
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Days, fromdate=datetime(2018, 1, 1), LiveBars=False)  # Исторические дневные бары с заданной даты
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, LiveBars=False)  # Исторические минутные бары за все время
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, fromdate=datetime(2021, 3, 16, 7, 0), compression=1, LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
