from datetime import date
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

# Получение данных одного тикера по разным временным интервалам методом конвертации меньшего временнОго интевала в больший (Resample)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    store = QKStore()  # Хранилище QUIK
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=date.today())  # Исторические данные по самому меньшему временному интервалу
    cerebro.adddata(data)  # Добавляем данные из QUIK для проверки исходников
    cerebro.resampledata(data, timeframe=TimeFrame.Minutes, compression=5, boundoff=1)  # Можно добавить больший временной интервал кратный меньшему (добавляется автоматом)
    data1 = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=date.today())
    cerebro.adddata(data1)  # Добавляем данные из QUIK для проверки правильности работы Resample
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
