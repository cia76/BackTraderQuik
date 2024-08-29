import logging  # Будем вести лог
import collections
from datetime import datetime, date

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderQuik import QKStore


# noinspection PyArgumentList
class MetaQKBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaQKBroker, cls).__init__(name, bases, dct)  # Инициализируем класс брокера
        QKStore.BrokerCls = cls  # Регистрируем класс брокера в хранилище QUIK


# noinspection PyProtectedMember,PyArgumentList
class QKBroker(with_metaclass(MetaQKBroker, BrokerBase)):
    """Брокер QUIK"""
    logger = logging.getLogger('QKBroker')  # Будем вести лог

    params = (
        ('lots', True),  # Входящий остаток в лотах (задается брокером)
        ('slippage_steps', 10),  # Кол-во шагов цены для проскальзывания
        # По статье https://zen.yandex.ru/media/id/5e9a612424270736479fad54/bitva-s-finam-624f12acc3c38f063178ca95
        ('client_code_for_orders', None),  # Номер торгового терминала. У брокера Финам требуется для совершения торговых операций
    )

    def __init__(self, **kwargs):
        super(QKBroker, self).__init__()
        self.store = QKStore(**kwargs)  # Хранилище QUIK
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие все свободные средства
        self.startingvalue = self.value = 0  # Стартовая и текущая стоимость всех позиций
        self.trade_nums = {}  # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

        self.store.provider.on_trans_reply = self.on_trans_reply  # Ответ на транзакцию пользователя
        self.store.provider.on_trade = self.on_trade  # Получение новой / изменение существующей сделки

    def start(self):
        super(QKBroker, self).start()
        self.get_all_active_positions()  # Получаем все активные позиции

    def getcash(self, account_id=None):
        """Свободные средства по всем счетам, по счету"""
        if not self.store.BrokerCls:  # Если брокера нет в хранилище
            return 0
        money_limits = self.store.provider.get_money_limits()['data']  # Все денежные лимиты (остатки на счетах)
        if len(money_limits) == 0:  # Если денежных лимитов нет
            self.logger.error('getcash: QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return 0
        cash = 0  # Будем набирать свободные средства
        for account in self.store.provider.accounts:  # Пробегаемся по всем счетам (Коды клиента/Фирма/Счет)
            if account_id is not None and account != self.store.provider.accounts[account_id]:  # Если считаем свободные средства по счету, и это не требуемый счет
                continue  # то переходим к следующему счету, дальше не продолжаем
            if account['futures']:  # Для фьючерсов
                # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
                # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
                # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
                # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
                # Накоплен.доход включает Биржевые сборы
                # Тек.чист.поз. = Заблокированное ГО под открытые позиции
                # План.чист.поз. = На какую сумму можете открыть еще позиции
                try:
                    futures_limit = self.store.provider.get_futures_limit(account['firm_id'], account['trade_account_id'], 0, self.store.provider.currency)['data']  # Фьючерсные лимиты по денежным средствам (limit_type=0)
                    cash += futures_limit['cbplimit'] + futures_limit['varmargin'] + futures_limit['accruedint']  # Добавляем свободные средства = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
                except Exception:  # При ошибке Futures limit returns nil
                    self.logger.error(f'getcash: QUIK не вернул фьючерсные лимиты с firm_id={account["firm_id"]}, trade_account_id={account["trade_account_id"]}. Проверьте правильность значений')
            else:  # Для остальных фирм
                current_balance = next((money_limit['currentbal'] for money_limit in money_limits  # Из всех денежных лимитов
                                       if money_limit['client_code'] == account['client_code'] and  # выбираем по коду клиента
                                       money_limit['firmid'] == account['firm_id'] and  # фирме
                                       money_limit['limit_kind'] == self.store.provider.limit_kind and  # дню лимита
                                       money_limit['currcode'] == self.store.provider.currency),  # и валюте
                                       None)  # получаем денежный лимит (остаток) по счету если он есть
                if current_balance is None:  # Если денежный лимит не найден
                    self.logger.error(f'getcash: Денежный лимит не найден с client_code={account["client_code"]}, firmid={account["firm_id"]}, limit_kind={self.store.provider.limit_kind}, currcode={self.store.provider.currency}. Проверьте правильность значений')
                else:  # Денежный лимит найден
                    cash += current_balance  # Добавляем свободные средства = денежный лимит
        if account_id is None and cash:  # Если были получены все свободные средства
            self.cash = cash  # то сохраняем все свободные средства
        return self.cash

    def getvalue(self, datas=None, account_id=None):
        """Стоимость всех позиций, позиции/позиций, по счету"""
        if not self.store.BrokerCls:  # Если брокера нет в хранилище
            return 0
        value = 0  # Будем набирать стоимость позиций
        for dataname, position in list(self.positions.items()):  # Пробегаемся по копии позиций (чтобы не было ошибки при изменении позиций)
            if datas and not next((data for data in datas if data._name == dataname), None):  # Если смотрим стоимость позиции/позиций, и это не заданный тикер
                continue  # то переходим к следующей позиции, дальше не продолжаем
            class_code, sec_code = self.store.provider.dataname_to_class_sec_codes(dataname)  # Получаем код режима торгов и тикер из названия тикера
            account = next((account for account in self.store.provider.accounts if class_code in account['class_codes']), None)  # По коду режима находим счет
            if account_id is not None and account != self.store.provider.accounts[account_id]:  # Если смотрим стоимость по счету, и это не заданный счет
                continue  # то переходим к следующей позиции, дальше не продолжаем
            last_price = self.store.provider.quik_price_to_price(class_code, sec_code, float(self.store.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value']))  # Последняя цена сделки в рублях за штуку
            value += position.size * last_price  # Добавляем стоимость позиции
        if datas is None and account_id is None and value:  # Если была получена стоимость всех позиций
            self.value = value  # то сохраняем стоимость всех позиций
        return self.value

    def getposition(self, data):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[data._name]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на покупку на биржу
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на продажу на биржу
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        if not self.notifs:  # Если в списке уведомлений ничего нет
            return None  # то ничего и возвращаем, выходим, дальше не продолжаем
        return self.notifs.popleft()  # Удаляем и возвращаем крайний левый элемент списка уведомлений

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(QKBroker, self).stop()
        self.store.provider.on_connected = self.store.provider.default_handler  # Соединение терминала с сервером QUIK
        self.store.provider.on_disconnected = self.store.provider.default_handler  # Отключение терминала от сервера QUIK
        self.store.provider.on_trans_reply = self.store.provider.default_handler  # Ответ на транзакцию пользователя
        self.store.provider.on_trade = self.store.provider.default_handler  # Получение новой / изменение существующей сделки
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_all_active_positions(self):
        """Все активные позиции"""
        for account in self.store.provider.accounts:  # Пробегаемся по всем счетам (Коды клиента/Фирма/Счет)
            if account['futures']:  # Для фьючерсов
                active_futures_holdings = [futures_holding for futures_holding in self.store.provider.get_futures_holdings()['data'] if futures_holding['totalnet'] != 0]  # Активные фьючерсные позиции
                for active_futures_holding in active_futures_holdings:  # Пробегаемся по всем активным фьючерсным позициям
                    class_code = 'SPBFUT'  # Код режима торгов для фьючерсов
                    sec_code = active_futures_holding['sec_code']  # Код тикера
                    dataname = self.store.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
                    size = int(active_futures_holding['totalnet'])  # Кол-во
                    if self.p.lots:  # Если входящий остаток в лотах
                        size = self.store.provider.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                    price = self.store.provider.quik_price_to_price(class_code, sec_code, float(active_futures_holding['avrposnprice']))  # Переводим эффективную цену позиций (входа) в цену в рублях за штуку
                    self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций
            else:  # Для остальных фирм
                depo_limits = self.store.provider.get_all_depo_limits()['data']  # Все лимиты по бумагам (позиции по инструментам)
                account_depo_limits = [depo_limit for depo_limit in depo_limits  # Бумажный лимит
                                       if depo_limit['client_code'] == account['client_code'] and  # выбираем по коду клиента
                                       depo_limit['firmid'] == account['firm_id'] and  # фирме
                                       depo_limit['limit_kind'] == self.store.provider.limit_kind and  # дню лимита
                                       depo_limit['currentbal'] != 0]  # только открытые позиции
                for firm_kind_depo_limit in account_depo_limits:  # Пробегаемся по всем позициям
                    class_code, sec_code = self.store.provider.dataname_to_class_sec_codes(firm_kind_depo_limit['sec_code'])  # По коду тикера без кода режима торгов получаем код режима торгов и тикера
                    dataname = self.store.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
                    size = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                    if self.p.lots:  # Если входящий остаток в лотах
                        size = self.store.provider.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
                    price = self.store.provider.quik_price_to_price(class_code, sec_code, float(firm_kind_depo_limit['wa_position_price']))  # Переводим средневзвешенную цену приобретения позиции (входа) в цену в рублях за штуку
                    self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций

    def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок"""
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные свойства из брокера, в т.ч. account_id
        class_code = data.class_code  # Код режима торгов
        sec_code = data.sec_code  # Тикер
        if 'account_id' in order.info:  # Если передали номер счета
            account = self.store.provider.accounts[order.info['account_id']]  # то получаем счет по номеру
            if class_code not in account['class_codes']:  # Если в этом счете нет режима торгов тикера
                account = None  # то счет не найден
        else:  # Если не передали номер счета
            account = next((account for account in self.store.provider.accounts if class_code in account['class_codes']), None)  # то ищем первый счет с режимом торгов тикера
        if not account:  # Если счет не найден
            self.logger.error(f'create_order: Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Не найден счет для режима торгов {class_code}')
            order.reject(self)  # то отменяем заявку (статус Order.Rejected)
            return order  # Возвращаем отмененную заявку
        order.addinfo(account=account)  # Передаем в заявку счет
        si = self.store.provider.get_symbol_info(class_code, sec_code)  # Получаем параметры тикера (min_price_step, scale)
        if not si:  # Если тикер не найден
            self.logger.error(f'create_order: Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Тикер не найден')
            order.reject(self)  # то отменяем заявку (статус Order.Rejected)
            return order  # Возвращаем отмененную заявку
        order.addinfo(min_price_step=float(si['min_price_step']))  # Передаем в заявку минимальный шаг цены
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                self.logger.error(f'create_order: Постановка заявки {order.ref} по тикеру {class_code}.{sec_code} отменена. Родительская заявка не найдена')
                order.reject(self)  # то отменяем заявку (статус Order.Rejected)
                return order  # Возвращаем отмененную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
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
        class_code = order.data.class_code  # Получаем из заявки код режима торгов
        sec_code = order.data.sec_code  # Получаем из заявки код тикера
        quantity = abs(order.size if order.data.derivative else self.store.provider.size_to_lots(class_code, sec_code, order.size))  # Размер позиции в лотах. В QUIK всегда передается положительный размер лота
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            # Если для заявок брокер устанавливает отдельный код клиента, то задаем его в параметре client_code_for_orders, и используем здесь
            # В остальных случаях получаем код клиента из заявки (счета). Для фьючерсов кода клиента нет
            'CLIENT_CODE': self.p.client_code_for_orders if self.p.client_code_for_orders else order.info['account']['client_code'],
            'ACCOUNT': order.info['account']['trade_account_id'],  # Получаем из заявки счет
            'CLASSCODE': class_code,  # Код режима торгов
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'QUANTITY': str(quantity),  # Кол-во в лотах
            'ACTION': 'NEW_ORDER' if order.exectype in (Order.Market, Order.Limit) else 'NEW_STOP_ORDER'}  # Заявка или стоп заявка
        min_price_step = order.info['min_price_step']  # Получаем из заявки минимальный шаг цены
        slippage = min_price_step * self.p.slippage_steps  # Размер проскальзывания в деньгах для выставления рыночной цены фьючерсов
        if order.exectype == Order.Market:  # Рыночная заявка
            transaction['TYPE'] = 'M'  # Рыночная заявка
            if order.data.derivative:  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
                last_price = float(self.store.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
                market_price = self.store.provider.price_to_valid_price(class_code, sec_code, last_price + slippage if order.isbuy() else last_price - slippage)  # Цена хуже последней сделки
            else:  # Для остальных рынков
                market_price = 0  # Цена рыночной заявки должна быть нулевой
            transaction['PRICE'] = str(market_price)  # Рыночная цена QUIK
            order.price = market_price  # Сохраняем в заявку рыночную цену заявки
        elif order.exectype == Order.Limit:  # Лимитная заявка
            transaction['TYPE'] = 'L'  # Лимитная заявка
            limit_price = self.store.provider.price_to_valid_price(class_code, sec_code, order.price) if order.data.derivative else self.store.provider.price_to_quik_price(class_code, sec_code, order.price)  # Лимитная цена
            transaction['PRICE'] = str(limit_price)  # Лимитная цена QUIK
            order.price = limit_price  # Сохраняем в заявку лимитную цену заявки
        elif order.exectype == Order.Stop:  # Стоп заявка
            stop_price = self.store.provider.price_to_valid_price(class_code, sec_code, order.price) if order.data.derivative else self.store.provider.price_to_quik_price(class_code, sec_code, order.price)  # Стоп цена
            transaction['STOPPRICE'] = str(stop_price)  # Стоп цена QUIK
            if order.data.derivative:  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
                market_price = self.store.provider.price_to_valid_price(class_code, sec_code, stop_price + slippage if order.isbuy() else stop_price - slippage)  # Цена хуже последней сделки
            else:  # Для остальных рынков
                market_price = 0  # Цена рыночной заявки должна быть нулевой
            transaction['PRICE'] = str(market_price)  # Рыночная цена QUIK
            order.price = stop_price  # Сохраняем в заявку стоп цену заявки
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            stop_price = self.store.provider.price_to_valid_price(class_code, sec_code, order.price) if order.data.derivative else self.store.provider.price_to_quik_price(class_code, sec_code, order.price)  # Стоп цена
            transaction['STOPPRICE'] = str(stop_price)  # Стоп цена QUIK
            limit_price = self.store.provider.price_to_valid_price(class_code, sec_code, order.pricelimit) if order.data.derivative else self.store.provider.price_to_quik_price(class_code, sec_code, order.pricelimit)  # Лимитная цена
            transaction['PRICE'] = str(limit_price)  # Лимитная цена QUIK
            order.price = stop_price  # Сохраняем в заявку стоп цену заявки
            order.pricelimit = limit_price  # Сохраняем в заявку лимитную цену заявки
        if order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп заявок
            expiry_date = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiry_date = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                expiry_date = order.valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction['EXPIRY_DATE'] = expiry_date  # Срок действия стоп заявки
        response = self.store.provider.send_transaction(transaction)  # Отправляем транзакцию на биржу
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        if response['cmd'] == 'lua_transaction_error':  # Если возникла ошибка при постановке заявки на уровне QUIK
            self.logger.error(f'place_order: Ошибка отправки заявки в QUIK {response["data"]["CLASSCODE"]}.{response["data"]["SECCODE"]} {response["lua_error"]}')  # то заявка не отправляется на биржу, выводим сообщение об ошибке
            order.reject(self)  # Отклоняем заявку (Order.Rejected)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if order.ref not in self.orders:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        order_num = order.info['order_num']  # Получаем из заявки номер заявки на бирже
        is_stop = order.exectype in [Order.Stop, Order.StopLimit] and isinstance(self.store.provider.get_order_by_number(order_num)['data'], int)  # Задана стоп заявка и лимитная заявка не выставлена
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': order.data.class_code,  # Получаем из заявки код режима торгов
            'SECCODE': order.data.sec_code}  # Получаем из заявки код тикера
        if is_stop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'  # Будем удалять стоп заявку
            transaction['STOP_ORDER_KEY'] = str(order_num)  # Номер стоп заявки на бирже
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'  # Будем удалять лимитную заявку
            transaction['ORDER_KEY'] = str(order_num)  # Номер заявки на бирже
        self.store.provider.send_transaction(transaction)  # Отправляем транзакцию на биржу
        return order  # В список уведомлений ничего не добавляем. Ждем события OnTransReply

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        for order_ref, oco_ref in self.ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = self.ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def on_trans_reply(self, data):
        """Обработчик события ответа на транзакцию пользователя"""
        self.logger.debug(f'on_trans_reply: data={data}')  # Для отладки
        qk_trans_reply = data['data']  # Ответ на транзакцию
        order_num = int(qk_trans_reply['order_num'])  # Номер заявки на бирже
        trans_id = int(qk_trans_reply['trans_id'])  # Номер транзакции заявки
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            self.logger.debug(f'on_trans_reply: Заявка с номером {order_num} выставлена не из автоторговли / только что. Выход')
            return  # не обрабатываем, пропускаем
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            self.logger.debug(f'on_trans_reply: Заявка с номером {order_num}. Номер транзакции {trans_id}. Заявка была выставлена не из торговой системы. Выход')
            return  # не обрабатываем, пропускаем
        order: Order = self.orders[trans_id]  # Ищем заявку по номеру транзакции
        order.addinfo(order_num=order_num)  # Передаем в заявку номер заявки на бирже
        self.logger.debug(f'on_trans_reply: Заявка {order.ref} с номером {order_num}. Номер транзакции {trans_id}. order={order}')
        # TODO Есть поле flags, но оно не документировано. Лучше вместо текстового результата транзакции разбирать по нему
        result_msg = str(qk_trans_reply['result_msg']).lower()  # По результату исполнения транзакции (очень плохое решение)
        status = int(qk_trans_reply['status'])  # Статус транзакции
        if status == 15 or 'зарегистрирован' in result_msg:  # Если пришел ответ по новой заявке
            self.logger.debug(f'on_trans_reply: Заявка {order.ref} переведена в статус принята на бирже (Order.Accepted)')
            order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        elif 'снят' in result_msg:  # Если пришел ответ по отмене существующей заявки
            try:  # TODO В BT очень редко при order.cancel() возникает ошибка:
                #    order.py, line 487, in cancel
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                self.logger.debug(f'on_trans_reply: Заявка {order.ref} переведена в статус отменена (Order.Canceled)')
                order.cancel()  # Отменяем существующую заявку (Order.Canceled)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Canceled  # все равно ставим статус заявки Order.Canceled
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):  # Транзакция не выполнена (ошибка заявки):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if status == 4 and 'не найдена заявка' in result_msg or \
               status == 5 and 'не можете снять' in result_msg or 'превышен лимит' in result_msg:
                self.logger.debug(f'on_trans_reply: Заявка {order.ref}. Ошибка. Выход')
                return  # то заявку не отменяем, выходим, дальше не продолжаем
            try:  # TODO В BT очень редко при order.reject() возникает ошибка:
                #    order.py, line 480, in reject
                #    self.executed.dt = self.data.datetime[0]
                #    linebuffer.py, line 163, in __getitem__
                #    return self.array[self.idx + ago]
                #    IndexError: array index out of range
                self.logger.debug(f'on_trans_reply: Заявка {order.ref} переведена в статус отклонена (Order.Rejected)')
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
                self.logger.debug(f'on_trans_reply: Заявка {order.ref} переведена в статус не прошла проверку лимитов (Order.Margin)')
                order.margin()  # Для заявки не хватает средств (Order.Margin)
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Margin  # все равно ставим статус заявки Order.Margin
        self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:  # Если новая заявка не зарегистрирована
            self.logger.debug(f'on_trans_reply: Заявка {order.ref}. Проверка связанных и родительских/дочерних заявок')
            self.oco_pc_check(order)  # то проверяем связанные и родительскую/дочерние заявки (Canceled, Rejected, Margin)
        self.logger.debug(f'on_trans_reply: Заявка {order.ref}. Выход')

    def on_trade(self, data):
        """Обработчик события получения новой / изменения существующей сделки.
        Выполняется до события изменения существующей заявки. Нужен для определения цены исполнения заявок.
        """
        self.logger.debug(f'on_trade: data={data}')  # Для отладки
        qk_trade = data['data']  # Сделка в QUIK
        trade_num = int(qk_trade['trade_num'])  # Номер сделки (дублируется 3 раза)
        order_num = int(qk_trade['order_num'])  # Номер заявки на бирже
        trans_id = int(qk_trade['trans_id'])  # Номер транзакции из заявки на бирже. Не используем GetOrderByNumber, т.к. он может вернуть 0
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            self.logger.debug(f'on_trade: Заявка с номером {order_num} выставлена не из автоторговли / только что. Выход')
            return  # выходим, дальше не продолжаем
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            self.logger.debug(f'on_trade: Заявка с номером {order_num}. Номер транзакции {trans_id}. Заявка была выставлена не из торговой системы. Выход')
            return  # выходим, дальше не продолжаем
        order: Order = self.orders[trans_id]  # Ищем заявку по номеру транзакции
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        self.logger.debug(f'on_trade: Заявка {order.ref} с номером {order_num}. Номер транзакции {trans_id}. Номер сделки {trade_num} order={order}')
        class_code = qk_trade['class_code']  # Код режима торгов
        sec_code = qk_trade['sec_code']  # Код тикера
        dataname = self.store.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и коду тикера
        if dataname not in self.trade_nums.keys():  # Если это первая сделка по тикеру
            self.trade_nums[dataname] = []  # то ставим пустой список сделок
        elif trade_num in self.trade_nums[dataname]:  # Если номер сделки есть в списке (фильтр для дублей)
            self.logger.debug(f'on_trade: Заявка {order.ref}. Номер сделки {trade_num} есть в списке сделок (дубль). Выход')
            return  # то выходим, дальше не продолжаем
        self.trade_nums[dataname].append(trade_num)  # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        size = int(qk_trade['qty'])  # Абсолютное кол-во
        if self.p.lots:  # Если входящий остаток в лотах
            size = self.store.provider.lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
        if qk_trade['flags'] & 0b100 == 0b100:  # Если сделка на продажу (бит 2)
            size *= -1  # то кол-во ставим отрицательным
        price = self.store.provider.quik_price_to_price(class_code, sec_code, float(qk_trade['price']))  # Переводим цену QUIK в цену в рублях за штуку
        self.logger.debug(f'on_trade: Заявка {order.ref}. size={size}, price={price}')
        try:  # TODO Очень редко возникает ошибка:
            #    linebuffer.py, line 163, in __getitem__
            #    return self.array[self.idx + ago]
            #    IndexError: array index out of range
            dt = order.data.datetime[0]  # Дата и время исполнения заявки. Последняя известная
            self.logger.debug(f'on_trade: Заявка {order.ref}. Дата/время исполнения заявки по бару {dt}')
        except (KeyError, IndexError):  # При ошибке
            dt = datetime.now(self.store.provider.tz_msk)  # Берем текущее время на бирже из локального
            self.logger.debug(f'on_trade: Заявка {order.ref}. Дата/время исполнения заявки по текущему {dt}')
        position = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = position.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если заявка исполнена частично (осталось что-то к исполнению)
            self.logger.debug(f'on_trade: Заявка {order.ref} исполнилась частично. Остаток к исполнения {order.executed.remsize}')
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                self.logger.debug(f'on_trade: Заявка {order.ref} переведена в статус частично исполнена (Order.Partial)')
                order.partial()  # Переводим заявку в статус Order.Partial
                self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если заявка исполнена полностью (ничего нет к исполнению)
            self.logger.debug(f'on_trade: Заявка {order.ref} переведена в статус полностью исполнена (Order.Completed)')
            order.completed()  # Переводим заявку в статус Order.Completed
            self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.logger.debug(f'on_trade: Заявка {order.ref}. Проверка связанных и родительских/дочерних заявок')
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
        self.logger.debug(f'on_trade: Заявка {order.ref}. Выход')
