from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

# Точное тестирование большего временного интервала с использованием меньшего (Replay)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    store = QKStore()  # Хранилище QUIK
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5)  # Исторические данные по меньшему временному интервалу
    cerebro.replaydata(data, timeframe=TimeFrame.Days)  # На графике видим большой интервал, прогоняем ТС на меньшем
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
