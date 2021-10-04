from datetime import datetime
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    symbols = ('TQBR.SBER', 'TQBR.GAZP', 'TQBR.LKOH', 'TQBR.GMKN',)  # Кортеж тикеров
    store = QKStore()  # Хранилище QUIK на локальном компьютере
    # store = QKStore(Host='192.168.1.7')  # Хранилище QUIK на удаленном компьютере
    for symbol in symbols:  # Пробегаемся по всем тикерам
        data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2021, 10, 4), LiveBars=True)  # Исторические и новые бары по первому тикеру
        cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars, name="One Ticker", symbols=('TQBR.SBER',))  # Добавляем торговую систему по одному тикеру
    cerebro.addstrategy(ts.PrintStatusAndBars, name="Two Tickers", symbols=('TQBR.GAZP', 'TQBR.LKOH',))  # Добавляем торговую систему по двум тикерам
    cerebro.addstrategy(ts.PrintStatusAndBars, name="All Tickers")  # Добавляем торговую систему по всем тикерам

    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
