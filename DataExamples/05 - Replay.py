from backtrader import Cerebro, TimeFrame
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK
import Strategy as ts  # Торговые системы

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = Cerebro()  # Инициируем "движок" BackTrader

    # Тестирование на меньшем временнОм интервале (Replay)
    symbol = 'TQBR.GAZP'
    store = QKStore(Host='192.168.1.7')
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5)  # Исторические данные по самому меньшему временному интервалу
    cerebro.replaydata(data, timeframe=TimeFrame.Days)  # На графике видим большой интервал, прогоняем ТС на меньшем
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему

    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график. Требуется matplotlib версии 3.2.2 (pip install matplotlib==3.2.2)
