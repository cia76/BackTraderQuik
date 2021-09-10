from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Несколько тикеров, один временной интервал
    symbol1 = 'TQBR.GAZP'
    symbol2 = 'TQBR.LKOH'
    store = QKStore()  # Хранилище QUIK на локальном компьютере
    # store = QKStore(Host='192.168.1.7')  # Хранилище QUIK на удаленном компьютере
    data = store.getdata(dataname=symbol1, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2021, 9, 1), LiveBars=True)  # Исторические и новые бары по первому тикеру
    cerebro.adddata(data)  # Добавляем данные
    data = store.getdata(dataname=symbol2, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2021, 9, 1), LiveBars=True)  # Исторические и новые бары по второму тикеру
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
