from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Несколько временнЫх интервалов, прямая загрузка
    symbol = 'TQBR.GAZP'
    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # store = QKStore(Host='<Ваш IP адрес>')  # Хранилище QUIK (К QUIK на удаленном компьютере обращаемся по IP или названию)
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2020, 1, 1))  # Исторические данные по малому временнОму интервалу (должен идти первым)
    cerebro.adddata(data)  # Добавляем данные
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Days, fromdate=datetime(2020, 1, 1))  # Исторические данные по большому временнОму интервалу
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
