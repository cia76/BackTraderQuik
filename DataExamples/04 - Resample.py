from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Несколько временнЫх интервалов: получение большего временнОго интервала из меньшего (Resample)
    symbol = 'TQBR.GAZP'
    store = QKStore(Host='192.168.1.7')
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=15, fromdate=datetime(2018, 1, 1))  # Исторические данные по самому меньшему временному интервалу
    cerebro.adddata(data)  # Добавляем данные
    cerebro.resampledata(data, timeframe=TimeFrame.Days)  # Можно добавить больший временной интервал кратный меньшему (добавляется автоматом)
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
