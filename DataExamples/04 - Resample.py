from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Несколько временнЫх интервалов: получение большего временнОго интервала из меньшего (Resample)
    symbol = 'TQBR.GAZP'
    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # store = QKStore(Host='<Ваш IP адрес>')  # Хранилище QUIK (К QUIK на удаленном компьютере обращаемся по IP или названию)
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2021, 11, 19))  # Исторические данные по самому меньшему временному интервалу
    cerebro.adddata(data)  # Добавляем данные из QUIK для проверки исходников
    cerebro.resampledata(data, timeframe=TimeFrame.Minutes, compression=5, boundoff=1)  # Можно добавить больший временной интервал кратный меньшему (добавляется автоматом)
    data1 = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=datetime(2021, 11, 19))
    cerebro.adddata(data1)  # Добавляем данные из QUIK для проверки правильности работы Resample
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
