from datetime import date
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

# Получение данных одного тикера по разным временнЫм интервалам методом прямой загрузки из истории
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    store = QKStore()  # Хранилище QUIK
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=date.today())  # Исторические данные по малому временнОму интервалу (должен идти первым)
    cerebro.adddata(data)  # Добавляем данные
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=date.today())  # Исторические данные по большому временнОму интервалу
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
