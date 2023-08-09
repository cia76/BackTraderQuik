from datetime import time, datetime
from backtrader import Cerebro, feeds, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

# Склейка тикера из файла и истории (Rollover)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер истории QUIK
    d1 = feeds.GenericCSVData(  # Получаем историю из файла
        dataname=f'..\\..\\Data\\{symbol}_D1.txt',  # Файл для импорта из QUIK. Создается из примера QuikPy Bars.py
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        sessionend=time(0, 0),  # Для дневных данных и выше подставляется время окончания сессии. Чтобы совпадало с историей, нужно поставить закрытие на 00:00
        fromdate=datetime(2020, 1, 1))  # Начальная дата и время приема исторических данных (Входит)
    store = QKStore()  # Хранилище QUIK
    d2 = store.getdata(dataname=symbol, timeframe=TimeFrame.Days, fromdate=datetime(2022, 12, 1), LiveBars=False)  # Получаем историю из QUIK
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    cerebro.rolloverdata(d1, d2, name=symbol)  # Склеенный тикер
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
