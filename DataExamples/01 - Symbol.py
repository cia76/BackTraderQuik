from datetime import datetime, time
from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
# from BackTraderQuik.QKData import QKData  # Данные QUIK для вызвова напрямую (не рекомендуется)
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    symbol = 'TQBR.SBER'
    # symbol = 'SPBFUT.SiH3'  # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>

    # data = QKData(dataname=symbol, timeframe=TimeFrame.Days, Host='<Ваш IP адрес>')  # Можно вызывать данные напрямую (не рекомендуется)

    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # store = QKStore(Host='<Ваш IP адрес>')  # Хранилище QUIK (К QUIK на удаленном компьютере обращаемся по IP или названию)

    # 1. Все исторические дневные бары
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Days, LiveBars=False)

    # 2. Исторические часовые бары с дожи 4-х цен за год
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=60, fromdate=datetime(2023, 1, 1), todate=datetime(2023, 12, 31), FourPriceDoji=True, LiveBars=False)

    # 3. Исторические 30-и минутные бары с заданной даты до последнего бара
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=30, fromdate=datetime(2023, 1, 9), LiveBars=False)

    # 4. Исторические 5-и минутные бары первого часа сессиИ без первой 5-и минутки
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=datetime(2023, 1, 13, 10, 5), todate=datetime(2023, 1, 13, 10, 55), LiveBars=False)

    # 5. Исторические 5-и минутные бары первого часа сессиЙ без первой 5-и минутки
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=datetime(2023, 1, 9), todate=datetime(2023, 1, 14), sessionstart=time(10, 5), sessionend=time(11, 0), LiveBars=False)

    # 6. Исторические и новые минутные бары с начала сегодняшней сессии
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 1, 13), LiveBars=True)

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график
