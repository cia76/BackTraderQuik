import collections
from datetime import datetime, date
import time

from backtrader import BrokerBase
from backtrader.utils.py3 import with_metaclass
from backtrader import Order, BuyOrder, SellOrder

from BackTraderQuik import QKStore


class MetaQKBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaQKBroker, cls).__init__(name, bases, dct)  # Инициализируем класс брокера
        QKStore.BrokerCls = cls  # Регистрируем класс брокера в хранилище QUIK


class QKBroker(with_metaclass(MetaQKBroker, BrokerBase)):
    """Брокер QUIK"""
    # TODO Сделать обертку для поддержки множества брокеров
    # TODO Сделать пример постановки заявок по разным портфелям
    # Обсуждение решения: https://community.backtrader.com/topic/1165/does-backtrader-support-multiple-brokers
    # Пример решения: https://github.com/JacobHanouna/backtrader/blob/ccxt_multi_broker/backtrader/brokers/ccxtmultibroker.py

    params = (
        ('use_positions', True),  # При запуске брокера подтягиваются текущие позиции с биржи
        ('Lots', True),  # Входящий остаток в лотах (задается брокером)
        ('ClientCode', ''),  # Код клиента
        # По статье https://zen.yandex.ru/media/id/5e9a612424270736479fad54/bitva-s-finam-624f12acc3c38f063178ca95
        ('ClientCodeForOrders', ''),  # Номер торгового терминала. У брокера Финам требуется для совершения торговых операций
        ('FirmId', 'SPBFUT'),  # Фирма
        ('TradeAccountId', 'SPBFUT00PST'),  # Счет
        ('LimitKind', 0),  # День лимита
        ('CurrencyCode', 'SUR'),  # Валюта
        ('IsFutures', True),  # Фьючерсный счет
    )

    def __init__(self, **kwargs):
        super(QKBroker, self).__init__()
        if not self.p.ClientCodeForOrders:  # Для брокера Финам нужно вместо кода клиента
            self.p.ClientCodeForOrders = self.p.ClientCode  # указать Номер торгового терминала
        self.store = QKStore(**kwargs)  # Хранилище QUIK
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.tradeNums = dict()  # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = 0  # Стартовый и текущий баланс счета

    def start(self):
        super(QKBroker, self).start()
        self.startingcash = self.cash = self.getcash()  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = self.getvalue()  # Стартовый и текущий баланс счета
        if self.p.use_positions:  # Если нужно при запуске брокера получить текущие позиции на бирже
            self.store.get_positions(self.p.ClientCode, self.p.FirmId, self.p.LimitKind, self.p.Lots, self.p.IsFutures)  # То получаем их
        self.store.qpProvider.OnConnected = self.store.on_connected  # Соединение терминала с сервером QUIK
        self.store.qpProvider.OnDisconnected = self.store.on_disconnected  # Отключение терминала от сервера QUIK
        self.store.qpProvider.OnTransReply = self.on_trans_reply  # Ответ на транзакцию пользователя
        self.store.qpProvider.OnTrade = self.on_trade  # Получение новой / изменение существующей сделки

    def getcash(self):
        """Свободные средства по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            cash = self.store.get_money_limits(self.p.ClientCode, self.p.FirmId, self.p.TradeAccountId, self.p.LimitKind, self.p.CurrencyCode, self.p.IsFutures)
            if cash:  # Если свободные средства были получены
                self.cash = cash  # то запоминаем их
        return self.cash  # Возвращаем последние известные свободные средства

    def getvalue(self, datas=None):
        """Баланс счета"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        # TODO Выдавать баланс по тикерам (datas) как в Alor
        # TODO Выдавать весь баланс, если не указан параметры. Иначе, выдавать баланс по параметрам
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            limits = self.store.get_positions_limits(self.p.FirmId, self.p.TradeAccountId, self.p.IsFutures)
            if limits:  # Если стоимость позиций была получена
                self.value = self.getcash() + limits  # Баланс счета = свободные средства + стоимость позиций
        return self.value  # Возвращаем последний известный баланс счета

    def getposition(self, data):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.store.positions[data._name]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, ClientCode=self.p.ClientCodeForOrders, TradeAccountId=self.p.TradeAccountId, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на покупку на биржу
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, ClientCode=self.p.ClientCodeForOrders, TradeAccountId=self.p.TradeAccountId, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на продажу на биржу
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.store.cancel_order(order)

    def get_notification(self):
        if not self.notifs:  # Если в списке уведомлений ничего нет
            return None  # то ничего и возвращаем, выходим, дальше не продолжаем
        return self.notifs.popleft()  # Удаляем и возвращаем крайний левый элемент списка уведомлений

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(QKBroker, self).stop()
        self.store.qpProvider.OnConnected = self.store.qpProvider.DefaultHandler  # Соединение терминала с сервером QUIK
        self.store.qpProvider.OnDisconnected = self.store.qpProvider.DefaultHandler  # Отключение терминала от сервера QUIK
        self.store.qpProvider.OnTransReply = self.store.qpProvider.DefaultHandler  # Ответ на транзакцию пользователя
        self.store.qpProvider.OnTrade = self.store.qpProvider.DefaultHandler  # Получение новой / изменение существующей сделки
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """
        Создание заявки
        Привязка параметров счета и тикера
        Обработка связанных и родительской/дочерних заявок
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные свойства из брокера, в т.ч. ClientCode, TradeAccountId, StopOrderKind
        class_code, sec_code = self.store.data_name_to_class_sec_code(data._name)  # Из названия тикера получаем код площадки и тикера
        order.addinfo(ClassCode=class_code, SecCode=sec_code)  # Код площадки ClassCode и тикера SecCode
        si = self.store.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (min_price_step, scale)
        if not si:  # Если тикер не найден
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Тикер не найден')
            order.reject(self)  # то отменяем заявку (статус Order.Rejected)
            return order  # Возвращаем отмененную заявку
        order.addinfo(MinPriceStep=float(si['min_price_step']))  # Минимальный шаг цены
        order.addinfo(Slippage=float(si['min_price_step']) * self.store.p.StopSteps)  # Размер проскальзывания в деньгах Slippage
        order.addinfo(Scale=int(si['scale']))  # Кол-во значащих цифр после запятой Scale
        if oco:  # Если есть связанная заявка
            self.store.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.store.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                print(f'Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Родительская заявка не найдена')
                order.reject(self)  # то отменяем заявку (статус Order.Rejected)
                return order  # Возвращаем отмененную заявку
            pcs = self.store.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self.place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим

    def place_order(self, order):
        """Отправка заявки (транзакции) на биржу"""
        class_code = order.info['ClassCode']  # Код площадки
        sec_code = order.info['SecCode']  # Код тикера
        size = abs(self.store.size_to_lots(class_code, sec_code, order.size))  # Размер позиции в лотах. В QUIK всегда передается положительный размер лота
        price = order.price  # Цена заявки
        if not price:  # Если цена не указана для рыночных заявок
            price = 0.00  # Цена рыночной заявки должна быть нулевой (кроме фьючерсов)
        slippage = order.info['Slippage']  # Размер проскальзывания в деньгах
        if slippage.is_integer():  # Целое значение проскальзывания мы должны отправлять без десятичных знаков
            slippage = int(slippage)  # поэтому, приводим такое проскальзывание к целому числу
        if order.exectype == Order.Market:  # Для рыночных заявок
            if class_code == 'SPBFUT':  # Для рынка фьючерсов
                last_price = float(self.store.qpProvider.GetParamEx(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
                price = last_price + slippage if order.isbuy() else last_price - slippage  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
        else:  # Для остальных заявок
            price = self.store.bt_to_quik_price(class_code, sec_code, price)  # Переводим цену из BackTrader в QUIK
        scale = order.info['Scale']  # Кол-во значащих цифр после запятой
        price = round(price, scale)  # Округляем цену до кол-ва значащих цифр
        if price.is_integer():  # Целое значение цены мы должны отправлять без десятичных знаков
            price = int(price)  # поэтому, приводим такую цену к целому числу
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLIENT_CODE': order.info['ClientCode'],  # Код клиента. Для фьючерсов его нет
            'ACCOUNT': order.info['TradeAccountId'],  # Счет
            'CLASSCODE': class_code,  # Код площадки
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(size)}  # Кол-во в лотах
        if order.exectype in [Order.Stop, Order.StopLimit]:  # Для стоп заявок
            transaction['ACTION'] = 'NEW_STOP_ORDER'  # Новая стоп заявка
            transaction['STOPPRICE'] = str(price)  # Стоп цена срабатывания
            plimit = order.pricelimit  # Лимитная цена исполнения
            if plimit:  # Если задана лимитная цена исполнения
                plimit = self.store.bt_to_quik_price(class_code, sec_code, plimit)  # Переводим цену из BackTrader в QUIK
                limit_price = round(plimit, scale)  # то ее и берем, округлив цену до кол-ва значащих цифр
            elif order.isbuy():  # Если цена не задана, и покупаем
                limit_price = price + slippage  # то будем покупать по большей цене в размер проскальзывания
            else:  # Если цена не задана, и продаем
                limit_price = price - slippage  # то будем продавать по меньшей цене в размер проскальзывания
            expiry_date = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiry_date = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                expiry_date = order.valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction['EXPIRY_DATE'] = expiry_date  # Срок действия стоп заявки
            if order.info['StopOrderKind'] == 'TAKE_PROFIT_STOP_ORDER':  # Если тип стоп заявки это тейк профит
                min_price_step = order.info['MinPriceStep']  # Минимальный шаг цены
                transaction['STOP_ORDER_KIND'] = order.info['StopOrderKind']  # Тип заявки TAKE_PROFIT_STOP_ORDER
                transaction['SPREAD_UNITS'] = 'PRICE_UNITS'  # Единицы измерения защитного спрэда в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['SPREAD'] = f'{min_price_step:.{scale}f}'  # Размер защитного спрэда. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
                transaction['OFFSET_UNITS'] = 'PRICE_UNITS'  # Единицы измерения отступа в параметрах цены (шаг изменения равен шагу цены по данному инструменту)
                transaction['OFFSET'] = f'{min_price_step:.{scale}f}'  # Размер отступа. Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
            else:  # Для обычных стоп заявок
                transaction['PRICE'] = str(limit_price)  # Лимитная цена исполнения
        else:  # Для рыночных или лимитных заявок
            transaction['ACTION'] = 'NEW_ORDER'  # Новая рыночная или лимитная заявка
            transaction['TYPE'] = 'L' if order.exectype == Order.Limit else 'M'  # L = лимитная заявка (по умолчанию), M = рыночная заявка
        response = self.store.qpProvider.SendTransaction(transaction)  # Отправляем транзакцию на биржу
        order.submit(self)  # Отправляем заявку на биржу (статус Order.Submitted)
        if response['cmd'] == 'lua_transaction_error':  # Если возникла ошибка при постановке заявки на уровне QUIK
            print(f'Ошибка отправки заявки в QUIK {response["data"]["CLASSCODE"]}.{response["data"]["SECCODE"]} {response["lua_error"]}')  # то заявка не отправляется на биржу, выводим сообщение об ошибке
            order.reject(self)  # Отклоняем заявку (Order.Rejected)
        self.store.orders[order.ref] = order  # Сохраняем в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def on_trans_reply(self, data):
        """Обработчик события ответа на транзакцию пользователя"""
        qk_trans_reply = data['data']  # Ответ на транзакцию
        trans_id = int(qk_trans_reply['trans_id'])  # Номер транзакции заявки
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            return  # не обрабатываем, пропускаем
        order_num = int(qk_trans_reply['order_num'])  # Номер заявки на бирже
        try:  # Могут приходить другие заявки, не выставленные в автоторговле
            order: Order = self.store.orders[trans_id]  # Ищем заявку по номеру транзакции
        except KeyError:  # При ошибке
            print(f'Заявка {order_num} на бирже с номером транзакции {trans_id} не найдена')
            return  # не обрабатываем, пропускаем
        self.store.orderNums[trans_id] = order_num  # Сохраняем номер заявки на бирже
        # TODO Есть поле flags, но оно не документировано. Лучше вместо текстового результата транзакции разбирать по нему
        result_msg = qk_trans_reply['result_msg']  # По результату исполнения транзакции (очень плохое решение)
        status = int(qk_trans_reply['status'])  # Статус транзакции
        if status == 15 or 'зарегистрирована' in result_msg:  # Если пришел ответ по новой заявке
            order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        elif 'снята' in result_msg:  # Если пришел ответ по отмене существующей заявки
            try:  # TODO В BT очень редко при order.cancel() возникает ошибка:
                #    order.py, line 487, in cancel
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                order.cancel()  # Отменяем существующую заявку (Order.Canceled)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Canceled  # все равно ставим статус заявки Order.Canceled
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):  # Транзакция не выполнена (ошибка заявки):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if status == 4 and 'Не найдена заявка' in result_msg or \
               status == 5 and 'не можете снять' in result_msg or 'Превышен лимит' in result_msg:
                return  # то заявку не отменяем, выходим, дальше не продолжаем
            try:  # TODO В BT очень редко при order.reject() возникает ошибка:
                #    order.py, line 480, in reject
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                order.reject(self)  # Отклоняем заявку (Order.Rejected)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Rejected  # все равно ставим статус заявки Order.Rejected
        elif status == 6:  # Транзакция не прошла проверку лимитов сервера QUIK
            try:  # TODO В BT очень редко при order.margin() возникает ошибка:
                #    order.py, line 492, in margin
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                order.margin()  # Для заявки не хватает средств (Order.Margin)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Margin  # все равно ставим статус заявки Order.Margin
        self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:  # Если новая заявка не зарегистрирована
            self.store.oco_pc_check(order)  # то проверяем связанные и родительскую/дочерние заявки

    def on_trade(self, data):
        """Обработчик события получения новой / изменения существующей сделки.
        Выполняется до события изменения существующей заявки. Нужен для определения цены исполнения заявок.
        """
        qk_trade = data['data']  # Сделка в QUIK
        order_num = int(qk_trade['order_num'])  # Номер заявки на бирже
        json_order = self.store.qpProvider.GetOrderByNumber(order_num)['data']  # По номеру заявки в сделке пробуем получить заявку с биржи
        if isinstance(json_order, int):  # Если заявка не найдена, то в ответ получаем целое число номера заявки. Возможно заявка есть, но она не успела прийти к брокеру
            print(f'Заявка с номером {order_num} не найдена на бирже с 1-ой попытки. Через 3 с будет 2-ая попытка')
            time.sleep(3)  # Ждем 3 секунды, пока заявка не придет к брокеру
            json_order = self.store.qpProvider.GetOrderByNumber(order_num)['data']  # Снова пробуем получить заявку с биржи по ее номеру
            if isinstance(json_order, int):  # Если заявка так и не была найдена
                print(f'Заявка с номером {order_num} не найдена на бирже со 2-ой попытки')
                return  # то выходим, дальше не продолжаем
        trans_id = int(json_order['trans_id'])  # Получаем номер транзакции из заявки с биржи
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            return  # не обрабатываем, пропускаем
        self.store.orderNums[trans_id] = order_num  # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        try:  # Бывает, что трейдеры совмещают авто и ручную торговлю. Это делать нельзя, но кто это будет слушать?
            order: Order = self.store.orders[trans_id]  # Ищем заявку по номеру транзакции
        except KeyError:  # Если пришла заявка из ручной торговли, то заявки по номеру транзакции в автоторговле не будет, получим ошибку
            print(f'Заявка с номером {order_num} и номером транзакции {trans_id} была выставлена не из торговой системы')
            return  # выходим, дальше не продолжаем
        class_code = qk_trade['class_code']  # Код площадки
        sec_code = qk_trade['sec_code']  # Код тикера
        dataname = self.store.class_sec_code_to_data_name(class_code, sec_code)  # Получаем название тикера по коду площадки и коду тикера
        trade_num = int(qk_trade['trade_num'])  # Номер сделки (дублируется 3 раза)
        if dataname not in self.tradeNums.keys():  # Если это первая сделка по тикеру
            self.tradeNums[dataname] = []  # то ставим пустой список сделок
        elif trade_num in self.tradeNums[dataname]:  # Если номер сделки есть в списке (фильтр для дублей)
            return  # то выходим, дальше не продолжаем
        self.tradeNums[dataname].append(trade_num)  # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        size = int(qk_trade['qty'])  # Абсолютное кол-во
        if self.p.Lots:  # Если входящий остаток в лотах
            size = self.store.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
        if qk_trade['flags'] & 0b100 == 0b100:  # Если сделка на продажу (бит 2)
            size *= -1  # то кол-во ставим отрицательным
        price = self.store.quik_to_bt_price(class_code, sec_code, float(qk_trade['price']))  # Переводим цену исполнения за лот в цену исполнения за штуку
        try:  # TODO Очень редко возникает ошибка:
            #    linebuffer.py, line 163, in __getitem__
            #    return self.array[self.idx + ago]
            #    IndexError: array index out of range
            dt = order.data.datetime[0]  # Дата и время исполнения заявки. Последняя известная
        except (KeyError, IndexError):  # При ошибке
            dt = datetime.now(QKStore.MarketTimeZone)  # Берем текущее время на бирже из локального
        pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если заявка исполнена частично (осталось что-то к исполнению)
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                order.partial()  # Переводим заявку в статус Order.Partial
                self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если заявка исполнена полностью (ничего нет к исполнению)
            order.completed()  # Переводим заявку в статус Order.Completed
            self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.store.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
