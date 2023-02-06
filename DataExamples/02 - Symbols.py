from datetime import date
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

# Несколько тикеров для нескольких торговых систем по одному временнОму интервалу
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbols = ('TQBR.SBER', 'TQBR.GAZP', 'TQBR.LKOH', 'TQBR.GMKN',)  # Кортеж тикеров
    store = QKStore()  # Хранилище QUIK
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    for symbol in symbols:  # Пробегаемся по всем тикерам
        data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=date.today(), LiveBars=True)  # Исторические и новые бары тикера с начала сессии
        cerebro.adddata(data)  # Добавляем тикер
    cerebro.addstrategy(ts.PrintStatusAndBars, name="One Ticker", symbols=('TQBR.SBER',))  # Добавляем торговую систему по одному тикеру
    cerebro.addstrategy(ts.PrintStatusAndBars, name="Two Tickers", symbols=('TQBR.GAZP', 'TQBR.LKOH',))  # Добавляем торговую систему по двум тикерам
    cerebro.addstrategy(ts.PrintStatusAndBars, name="All Tickers")  # Добавляем торговую систему по всем тикерам
    cerebro.run()  # Запуск торговой системы
