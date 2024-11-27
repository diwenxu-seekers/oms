import asyncio
import concurrent.futures
import logging
import math
import time
from asyncio import AbstractEventLoop
from collections import deque, OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Lock
from typing import Dict, List, Set, Tuple

import ujson
import zmq
from zmq.asyncio import Context, Poller

import gateway_lib as gl
from oms.common.config import (CFG_BROKER, CFG_BROKERS, CFG_CONNECTION, CFG_MESSAGING, CFG_NAME, CFG_NUM_OF_WORKERS,
                               CFG_OMS)
from oms.common.message import ErrorCode, MsgType, OmsMessage, OmsMessageError
from smartquant.common.config import CFG_LONG, CFG_SHORT
from smartquant.common.instrument import Instrument, InstrumentRepository
from smartquant.common.instrument import RollInstruction
from smartquant.common.market import Market
from smartquant.common.price import Price
from smartquant.execution.base import Action, OrderType, OrderState
from smartquant.strategy.base import DirtectionFactory
from .broker import Broker, BrokerFactory
from .ledger.factory import LedgerFactory
from .ledger.statement import TableInstrument, TableOrder, TablePortfolio, TablePosition, TablePositionByEntry
from .session import ClientSession


class Oms:
    STRATEGY_NAME = 'OMS'
    PING_INTERVAL = timedelta(seconds=5)

    FROM_GW_ORDER_TYPE: Dict[gl.OrderType, OrderType] = {
        gl.OrderType.MKT: OrderType.MKT,
        gl.OrderType.LMT: OrderType.LMT,
        gl.OrderType.STP: OrderType.STP,
        gl.OrderType.STP_LMT: OrderType.STP_LMT
    }

    TO_GW_ORDER_TYPE: Dict[OrderType, gl.OrderType] = {
        OrderType.MKT: gl.OrderType.MKT,
        OrderType.LMT: gl.OrderType.LMT,
        OrderType.STP: gl.OrderType.STP,
        OrderType.STP_LMT: gl.OrderType.STP_LMT
    }

    FROM_GW_ORDER_STATUS: Dict[str, OrderState] = {
        gl.OrderStatus.UNDEFINED: OrderState.INACTIVE,
        gl.OrderStatus.SUBMITTED: OrderState.ACTIVE,
        gl.OrderStatus.FILLED: OrderState.FULLY_FILLED,
        gl.OrderStatus.PARTIAL_FILLED: OrderState.ACTIVE,
        gl.OrderStatus.CANCELLED: OrderState.CANCELLED,
        gl.OrderStatus.INACTIVE: OrderState.INACTIVE,
        gl.OrderStatus.REJECTED: OrderState.REJECTED
    }

    FROM_ACTION: Dict[gl.OrderAction, bool] = {
        'BUY': True,
        'SELL': False
    }

    TO_ACTION: Dict[bool, gl.OrderAction] = {
        True: gl.OrderAction.BUY,
        False: gl.OrderAction.SELL
    }

    def __init__(self, config: OrderedDict):
        self._logger = logging.getLogger(__name__)

        self._lock = Lock()
        self._request_id = self._generate_request_id()
        self._logger.info(f'Initial request ID: {self._request_id}')

        self._config = config
        self._context = Context()
        cfg = self._config[CFG_MESSAGING][CFG_OMS]

        self._n_workers = int(cfg[CFG_NUM_OF_WORKERS])
        self._sessions: Dict[str, ClientSession] = dict()
        self._ledger = LedgerFactory.create_ledger(config)

        self._pending_messages = deque()

        self._brokers = dict()
        brokers = config[CFG_BROKERS]
        for b in brokers:
            broker_name = b[CFG_NAME]
            if broker_name in brokers:
                raise ValueError(f'Broker {broker_name} is duplicated')
            broker = BrokerFactory.create_broker(b)
            broker.gateway.events.on_error(self.handle_broker_error)
            broker.gateway.events.on_connection_update(self.handle_broker_connection_update)
            broker.gateway.events.on_order_update(self.handle_order_update)
            broker.gateway.events.on_execution(self.handle_execution)
            broker.gateway.events.on_account_info_update(self.handle_account_info_update)
            broker.gateway.events.on_position_update(self.handle_position_update)
            broker.gateway.events.on_open_order_end(self.handle_open_order_end)
            self._brokers[broker_name] = broker

        self._roll_orders: Set[int] = set()

    def init(self, loop: AbstractEventLoop):
        with concurrent.futures.ThreadPoolExecutor(len(self._brokers)) as pool:
            for n, b in self._brokers.items():
                self._logger.info(f'Connecting broker {n}...')
                loop.run_in_executor(pool, b.connect)

        self._roll_contracts()

    def close(self):
        self._logger.info('Shutting down OMS...')
        for n, b in self._brokers.items():
            self._logger.info(f'Disconnecting broker {n}...')
            b.disconnect()
        self._ledger.close()

    def get_broker(self) -> Broker:
        for _, broker in self._brokers.items():
            # TODO: return first broker for the time being, cause we got IB only anyway
            if broker.is_healthy:
                return broker

    def get_next_id(self) -> int:
        with self._lock:
            r = self._request_id
            self._request_id += 1
        return r

    def handle_open_order_end(self, src: gl.AbstractGateway, event: gl.OpenOrdersUpdate):
        # identify open order(s) that is cancelled without callback
        # is_historical means it is not triggered by a real time event
        # event.is_historical

        # syncchronize database records of open orders
        # by comparing against the open order snapshot of the broker

        def order_key1(x):
            return x.gateway_id + x.order_ref

        available_indexes = [order_key1(x) for x in event.open_orders if x.order_ref != None]

        self._logger.info(f'Open order(s): {event.open_orders}')

        def order_key2(x):
            return x[TableOrder.BROKER_ID] + x[TableOrder.BROKER_ORDER_ID]

        # clean up open entry orders that are not listed on broker's open order list
        orders = self._ledger.query_order(src.name,
            order_type=OrderType.LMT, action=Action.ENTRY, active_orders_only=True)
        for order in orders:
            key = order_key2(order)
            if key not in available_indexes:
                order_id = order[TableOrder.BROKER_ORDER_ID]
                session_id = order[TableOrder.SESSION_ID]
                session_order_id = order[TableOrder.ORDER_ID]
                # unfilled
                if order[TableOrder.FILLED_QUANTITY] == 0:
                    self._ledger.update_order(event.gateway_id, order_id, state=OrderState.CANCELLED)
                    self._ledger.delete_position_by_entry(session_id, session_order_id)
                    self._housekeep_expired_order(order_id)
                elif order[TableOrder.REMAINING_QUANTITY] > 0:
                    # partial filled is handled by `handle_order_update`
                    filled = order[TableOrder.FILLED_QUANTITY]
                    self._handle_partial_filled_order(
                        order_ref=order_id, broker_id=src.name,
                        session_id=session_id, session_order_id=session_order_id,
                        qty=filled, remaining=0, filled=filled, order=order)


    def handle_account_info_update(self, src: gl.AbstractGateway, event: gl.AccountUpdate):
        self._logger.debug(f'handle_account_info_update: {src}, {event}')

    def handle_broker_connection_update(self, src: gl.AbstractGateway, event: gl.ConnectionUpdate):
        self._logger.info(f'handle_broker_connection_update: {src}, {event}')

        broker = self._brokers[src.name]
        if event.status == gl.ConnectionStatus.CONNECTED:
            broker.is_connected = True
        elif event.status == gl.ConnectionStatus.DISCONNECTED:
            broker.is_connected = False

    def handle_broker_error(self, src: gl.AbstractGateway, event: gl.ErrorMessage):
        self._logger.info(f'handle_broker_error: {src}, {event}')

        if type(event) is gl.OrderError:
            order_id = int(event.order_id)
            s = self._lookup_session_by_order_id(order_id)

            #TODO: error code not exists on IB website e.g. 10147, 10149
            #TODO: there are more order error code e.g. 202
            if event.code == 10147:
                self._ledger.update_order(event.gateway_id, order_id, state=OrderState.INACTIVE)
            elif s is not None:
                self._logger.info(f'Order {order_id} belongs to session {s.id}')
                if event.code in [103, 107, 109, 110, 116, 200, 201, 10149]:
                    orders = self._ledger.query_order(broker_id=src.name,
                        broker_order_id=event.order_id, action=Action.ENTRY)
                    session_order_id = s.find_session_order_id(order_id)
                    if session_order_id is not None:
                        if len(orders) == 1:
                            # remove `position_by_entry` record of a rejected entry order
                            self._ledger.delete_position_by_entry(s.id, session_order_id)
                        # reject event would trigger strategy client to reset projected position
                        s.publish_order_rejected(session_order_id, event.msg)
                else:
                    s.publish_order_error(order_id, event.msg)
        else:
            if event.code in [502, 504, 1100]:
                # Connectivity between IB and Trader Workstation has been lost.
                broker = self._brokers[src.name]
                broker.is_connected = False
            elif event.code in [1101, 1102]:
                # Connectivity between IB and Trader Workstation has been restored.
                broker = self._brokers[src.name]
                broker.is_connected = True

    def handle_execution(self, src: gl.AbstractGateway, event: gl.ExecutionUpdate):
        self._logger.info(f'handle_execution: {src}, {event}')

        # only handle execution update originates by OMS
        if src.identity != event.client_id:
            self._logger.info(f"Ignore execution update due to client id is not '{src.identity}'")
            return

        executions = self._ledger.query_executions(src.name, event.exec_id)
        if executions:
            self._logger.info(f'Receive old execution: {src.name},{event.exec_id}, nothing needs to be done')
        else:
            self._logger.info(f'Process new execution: {src.name},{event.exec_id}')

            if event.order_ref is None or event.order_ref == '' or event.broker_order_id == 0:
                self._logger.info(f'Skip unknown order, either the order reference or the broker order ID is not '
                                  f'recognized: {event}')
                return

            is_buy = self.FROM_ACTION[event.side]
            direction = DirtectionFactory.build(CFG_LONG if is_buy else CFG_SHORT)
            self._ledger.insert_execution(src.name,
                event.order_ref, event.exec_id, event.broker_order_id,
                is_buy, event.symbol, event.filled, event.avg_price,
                None, event.commission, event.currency, event.timestamp)

            orders = self._ledger.query_order(broker_id=src.name, broker_order_id=event.order_ref)
            if len(orders) != 1:
                self._logger.critical(f'Cannot find the order with broker order ID {event.order_ref}, unable to update'
                                      f' position')
                return

            # Update position
            order = orders[0]

            market = Market[order[TableOrder.MARKET]]
            symbol = order[TableOrder.SYMBOL]
            portfolio = order[TableOrder.PORTFOLIO]
            strategy = order[TableOrder.STRATEGY]
            position = direction.quantity2position(event.filled)
            avg_price = event.avg_price
            order_quantity = int(order[TableOrder.QUANTITY])
            action = Action[order[TableOrder.ACTION]]

            fullyfilled = Decimal(str(order_quantity)) - Decimal(str(event.cum_qty)) == 0

            # Handle auto contract roll order
            if order[TableOrder.STRATEGY] == self.STRATEGY_NAME:
                self._logger.info(f'The order {event.order_ref} was sent by OMS, do not need to update position')
                if fullyfilled and int(event.order_ref) in self._roll_orders:
                    self._logger.info(f'The roll order {event.order_ref} has been filled completely')
                    self._roll_orders.remove(int(event.order_ref))
                return

            positions = self._ledger.query_position(portfolio_id=portfolio, strategy=strategy, market=str(market),
                                                    symbol=symbol)
            if len(positions) > 0:
                stgy_pos = positions[0]
                pos = stgy_pos[TablePosition.POSITION]
                if pos != 0:
                    existing_avg_price = float(stgy_pos[TablePosition.AVG_PRICE])
                    avg_price = ((avg_price * math.fabs(position) + existing_avg_price * math.fabs(pos)) / (
                            math.fabs(position) + math.fabs(pos)))
                    self._logger.info(f'There is an existing position of {pos}@{existing_avg_price}, compute the new '
                                      f'average price: {avg_price}')

            self._ledger.update_position(portfolio, strategy, str(market), symbol, position, avg_price)
            if fullyfilled:
                # In case there is no OrderUpdate event if order is executed when disconnected from TWS
                self._ledger.update_order(event.gateway_id, event.order_ref,
                    remaining_quantity=0, filled_quantity=order_quantity,
                    state=OrderState.FULLY_FILLED)

            session = self._lookup_session_by_order_id(int(event.order_ref))
            if session:
                self._logger.info(f'Order {event.order_ref} belongs to session {session.id}')
                session.publish_execution(event, order)
                session.publish_position()

            # Update stop-loss
            # TODO: support different way of sending stop, e.g. multiple stop orders
            if event.cum_qty == order_quantity:
                if action.is_entry():
                    self._logger.info(f'Entry order {event.order_ref} is fully filled, send stop-loss order. '
                                      f'Execution ID: {event.exec_id}')

                    instrument = InstrumentRepository().find(market=market, symbol=symbol)
                    is_buy = False if int(order[TableOrder.IS_BUY]) else True
                    comment = ujson.loads(order[TableOrder.COMMENT])
                    offset = float(comment[TableOrder.COMMENT_STOP_LOSS_OFFSET])
                    price = direction.nearest_worse_tick(Price(event.avg_price + offset), instrument)

                    try:
                        # absolute stop loss should be defined by client ONLY IF a customized stop price is intended
                        absolute = float(comment[TableOrder.COMMENT_STOP_LOSS_ABSOLUTE])
                        self._logger.info(f'Absolute stop-loss overrides stop-loss with offset, is buy: {is_buy}, '
                                            f'absolute stop-loss: {absolute}, stop-loss with offset: {price}')
                        price = absolute
                    except KeyError:
                        absolute = None

                    parent_order_id = int(order[TableOrder.ORDER_ID])
                    comment[TableOrder.COMMENT_COST] = event.avg_price

                    session_id = order[TableOrder.SESSION_ID]
                    order_id = order[TableOrder.ORDER_ID]

                    self._place_stop(session_id, market, symbol, is_buy, order_quantity, float(price), portfolio,
                                     strategy, parent_order_id, comment, session)
                    self._ledger.update_position_by_entry(session_id, order_id, avg_price=avg_price,
                                                          state=OrderState.FULLY_FILLED.value)
                elif action.is_exit():
                    try:
                        comment = ujson.loads(order[TableOrder.COMMENT])
                        order_ref = comment[TableOrder.COMMENT_ORDER_REFERENCE]
                    except (KeyError, TypeError):
                        order_ref = None

                    portfolio = order[TableOrder.PORTFOLIO]
                    strategy = order[TableOrder.STRATEGY]
                    session_id = order[TableOrder.SESSION_ID]
                    if order_ref is not None:
                        self._ledger.update_position_by_entry(portfolio_id=portfolio, strategy=strategy,
                                                              order_reference=order_ref, state='EXITED')
                    else:
                        entry_positions = self._ledger.query_position_by_entry(
                            portfolio_id=portfolio, strategy=strategy, market=str(market), symbol=symbol)

                        accumulated_quantity = 0
                        for p in reversed(entry_positions):
                            if accumulated_quantity == int(order[TableOrder.QUANTITY]):
                                break
                            elif accumulated_quantity > int(order[TableOrder.QUANTITY]):
                                self._logger.warning(f'Quantity of exit order ({accumulated_quantity}) excesses the '
                                                     f'sum of accumulated quantity of entry positions '
                                                     f'{accumulated_quantity}')
                                break

                            order_ref = p[TablePositionByEntry.ORDER_REFERENCE]
                            current_position = p[TablePositionByEntry.POSITION]
                            if order_quantity < current_position:
                                # must be partial exit
                                new_position = current_position - order_quantity
                                self._ledger.update_position_by_entry(portfolio_id=portfolio,
                                                                      strategy=strategy,
                                                                      order_reference=order_ref,
                                                                      position=new_position)
                                self._logger.info(f'Partial exit to update position_by_entry, current_position={current_position} new_position={new_position}')
                                orders = self._ledger.query_order(portfolio=portfolio, strategy=strategy,
                                                                  order_type=OrderType.STP,
                                                                  # active_orders_only=True,
                                                                  order_by_created=True)

                                for o in orders:
                                    try:
                                        stp_comment = ujson.loads(o[TableOrder.COMMENT])
                                        stp_order_ref = stp_comment.get(TableOrder.COMMENT_ORDER_REFERENCE)
                                        stp_qty = o[TableOrder.QUANTITY]
                                    except (TypeError, KeyError):
                                        continue
                                    if order_ref == stp_order_ref and current_position == stp_qty:
                                        parent_order_id = int(o[TableOrder.PARENT_ORDER_ID])
                                        self._place_stop(session_id, market, symbol, o[TableOrder.IS_BUY],
                                                         new_position,
                                                         float(o[TableOrder.PRICE]),
                                                         portfolio,
                                                         strategy,
                                                         parent_order_id,
                                                         ujson.loads(o[TableOrder.COMMENT]),
                                                         session)
                                        self._logger.info(f'Add new stop after partial exit, parent_order_id={parent_order_id}, qty={new_position}')
                                        break
                                accumulated_quantity += order_quantity
                                self._logger.warning(f'Partial exit on position_by_entry, new position is {current_position} - {order_quantity}')
                            else:
                                self._ledger.update_position_by_entry(portfolio_id=portfolio, strategy=strategy,
                                                                      order_reference=order_ref, state='EXITED')
                                accumulated_quantity += current_position
                                order_quantity -= current_position

    def _housekeep_expired_order(self, order_ref):
        # update strategy order cancelled to reset projected position.
        order_id = int(order_ref)
        s = self._lookup_session_by_order_id(order_id)
        if not s:
            self._logger.warning(f"Failed to find the session with order reference '{order_id}'")
            return
        session_order_id = s.find_session_order_id(order_id)
        if session_order_id is None:
            self._logger.warning(f"Failed to find the session order id with order reference '{order_id}'")
            return
        s.publish_order_rejected(session_order_id, "Order Cancelled")

    def _handle_partial_filled_order(self, order_ref, broker_id: str,
        session_id :str, session_order_id,
        qty, remaining, filled, order):
        """
        Treat partial filled LMT order as a fully filled order.
        """
        # update order to traded size
        self._ledger.update_order(broker_id, order_ref,
            quantity=qty, remaining_quantity=remaining, filled_quantity=filled,
            state=OrderState.FULLY_FILLED)

        # update position_by_entry to traded size, and avg. entry price
        self._ledger.update_position_by_entry(session_id, session_order_id,
            position=qty,
            avg_price=order[TableOrder.PRICE],
            state=OrderState.FULLY_FILLED.value)

        session = self._lookup_session_by_order_id(int(order_ref))
        if not session:
            self._logger.warning(f"Failed to find the session with order reference '{order_ref}'")
            return

        # place stop
        self._place_stop(session_id,
            Market[order[TableOrder.MARKET]],
            order[TableOrder.SYMBOL],
            int(order[TableOrder.IS_BUY]) == 0,     # reverse side of the order
            qty,
            float(order[TableOrder.PRICE]),
            order[TableOrder.PORTFOLIO],
            order[TableOrder.STRATEGY],
            int(order[TableOrder.PARENT_ORDER_ID]),
            ujson.loads(order[TableOrder.COMMENT]),
            session)

        # notify strategy clients with new position
        session.publish_position_renew()

    def handle_order_update(self, src: gl.AbstractGateway, event: gl.OrderUpdate):
        self._logger.info(f'handle_order_update: {src}, {event}')

        # only handle order update originates by OMS
        if src.identity != event.client_id:
            self._logger.info(f"Ignore order update due to client id is not '{src.identity}'")
            return

        # update position_by_entry for cancelled LMT order
        if event.status == gl.OrderStatus.CANCELLED and not event.is_historical:
            orders = self._ledger.query_order(src.name, broker_order_id=event.order_ref,
                order_type=OrderType.LMT, action=Action.ENTRY)
            if len(orders) == 1:
                order = orders[0]
                session_id = order[TableOrder.SESSION_ID]
                order_id = order[TableOrder.ORDER_ID]
                traded_size = event.filled
                if traded_size == 0:
                    # unfilled order
                    self._ledger.delete_position_by_entry(session_id, order_id)
                    self._housekeep_expired_order(event.order_ref)
                elif event.remaining > 0:
                    # partial fill
                    # will this double trigger?
                    self._handle_partial_filled_order(
                        order_ref=event.order_ref, broker_id=src.name,
                        session_id=session_id, session_order_id=order_id,
                        qty=traded_size, remaining=0, filled=traded_size, order=order)


        # TODO: check if it is a manual stop update
        orders = self._ledger.query_order(src.name, broker_order_id=event.order_ref, order_type=OrderType.STP)
        order_action = None
        if len(orders) == 1:
            order = orders[0]
            try:
                comment = ujson.loads(order[TableOrder.COMMENT])
                order_ref = comment[TableOrder.COMMENT_ORDER_REFERENCE]
            except (KeyError, TypeError):
                order_ref = None
                if TableOrder.COMMENT in order:
                    self._logger.warning(f'Cannot find order_ref from {order[TableOrder.COMMENT]}')
                else:
                    self._logger.warning(f'Cannot find comment from {str(order)}')

            stop_price = event.order.stop_price
            if not math.isclose(float(order[TableOrder.PRICE]), stop_price):
                self._logger.info(f'The price of the STOP order {event.order_ref} has been changed from '
                                  f'{order[TableOrder.PRICE]} to {stop_price}. Mark the order action to '
                                  f'manual-stop')
                order_action = Action.MANUAL_STOP_LOSS
                if order_ref:
                    self._ledger.insert_operation(portfolio_id=order[TableOrder.PORTFOLIO],
                                                  strategy=order[TableOrder.STRATEGY],
                                                  order_reference=order_ref,
                                                  position=0,
                                                  price=stop_price,
                                                  action=Action.AMEND,
                                                  identity=event.gateway_id)

            if not math.isclose(float(order[TableOrder.QUANTITY]), event.order.quantity):
                self._logger.info(f'The quantity of the STOP order {event.order_ref} has been changed from '
                                  f'{order[TableOrder.QUANTITY]} to {int(event.order.quantity)}. Mark the order action to '
                                  f'manual-stop, position will be corresponding updated.')
                session = self._lookup_session_by_order_id(int(event.order_ref))
                if session:
                    self._logger.debug(f'Order {event.order_ref} belongs to session {session.id}')
                    order_action = Action.MANUAL_STOP_LOSS
                    direction = -1 if (order[TableOrder.IS_BUY] == 1) else 1
                    adj_position_size = int(event.order.quantity) - order[TableOrder.QUANTITY]
                    self._ledger.update_position(order[TableOrder.PORTFOLIO], order[TableOrder.STRATEGY],
                                                 order[TableOrder.MARKET], order[TableOrder.SYMBOL],
                                                 position=adj_position_size*direction)
                    if order_ref:
                        self._logger.debug(f'Found order_ref={order_ref}, will be update_position_by_entry for manual operation')
                        if event.order.quantity < order[TableOrder.QUANTITY]:
                            action = Action.REDUCE
                        else:
                            action = Action.INCREASE
                        self._ledger.insert_operation(portfolio_id=order[TableOrder.PORTFOLIO],
                                                      strategy=order[TableOrder.STRATEGY],
                                                      order_reference=order_ref,
                                                      position=adj_position_size,
                                                      action=action,
                                                      identity=event.gateway_id)
                    session.publish_position_renew()
                    # session.publish_next_request_id()
                else:
                    self._logger.error(f'Cannot find any session own the Order {event.order_ref}')

        #TODO: refactor oms and database to process and store both limit price and stop price
        self._ledger.update_order(event.gateway_id, event.order_ref, event.order.quantity, event.order.price,
                                  event.remaining, event.filled, self.FROM_GW_ORDER_STATUS[event.status], order_action)


    def handle_position_update(self, src: gl.AbstractGateway, event: gl.PositionUpdate):
        self._logger.debug(f'handle_position_update: {src}, {event}')

    def install_loops(self, loop: AbstractEventLoop):
        asyncio.ensure_future(self.run(loop))

    def is_ready(self):
        for b in self._brokers.values():
            if not b.is_connected:
                return False
        return True

    def place_order(self, market: Market, symbol: str,
        order_type: OrderType, is_buy: bool, quantity: int, price: float,
        good_till: str=""):
        # Use the symbol directly if can't find in instrument repository, otherwise pick the front month contract
        order_symbol = symbol
        instrument = InstrumentRepository().find(market, symbol)
        if instrument is not None and instrument.symbol == symbol:
            order_symbol = instrument.front_month.symbol
            self._logger.info(
                f'Front month contract for symbol {symbol} is {order_symbol}, will send order with this symbol instead')

        gl_order_type = int(self.TO_GW_ORDER_TYPE[order_type])
        action = int(self.TO_ACTION[is_buy])

        broker = self.get_broker()

        if broker is None:
            self._logger.warning(f'Cannot find any available broker')
            return None, None

        req_id = self.get_next_id()

        rth = order_type in [OrderType.STP, OrderType.STP_LMT]
        _lmt_price, _stop_price = None, None
        if order_type == OrderType.STP:
            _stop_price = price
        elif order_type == OrderType.LMT:
            _lmt_price = price
        elif order_type == OrderType.STP_LMT:
            #TODO: handle lmt =/= stop
            _lmt_price = _stop_price = price

        tif=gl.TIF.GTC
        if good_till:
            tif = gl.TIF.GTD

        order = gl.Order(
            symbol=order_symbol,
            exchange=gl.Exchange.from_str(market.value),
            contractType=gl.ContractType.Future,
            orderType=gl_order_type,
            action=action,
            quantity=quantity,
            limit_price=_lmt_price,
            stop_price=_stop_price,
            tif=tif,
            outsideRth=rth,
            goodTillDate=good_till)
        self._logger.info(f'Send order to broker: {req_id},{repr(order)}')
        with self._lock:
            broker.place_order(f'{req_id}', order)

        return broker.name, req_id

    async def run(self, loop: AbstractEventLoop):
        self._logger.info(f'Start listening with {self._n_workers} workers...')

        cfg = self._config[CFG_MESSAGING][CFG_OMS]
        socket = self._context.socket(zmq.DEALER)
        broker_addr = cfg[CFG_CONNECTION][CFG_BROKER]
        self._logger.info(f'Connect to messaging proxy at {broker_addr}...')
        socket.connect(broker_addr)

        poller = Poller()
        poller.register(socket, zmq.POLLIN)
        future_results = []

        with concurrent.futures.ThreadPoolExecutor(self._n_workers) as pool:
            last_ping = datetime.min

            while loop.is_running():
                for f in list(future_results):
                    if f.done():
                        try:
                            result = f.result()
                            if result is not None:
                                self._logger.debug(f'OMS sends: {result}')
                                socket.send_multipart(result)
                        finally:
                            future_results.remove(f)

                while len(self._pending_messages) > 0:
                    msg = self._pending_messages.popleft()
                    self._logger.debug(f'OMS sends: {msg}')
                    socket.send_multipart(msg)

                socks = dict(await poller.poll(timeout=1))
                if socks.get(socket) == zmq.POLLIN:
                    msg = await socket.recv_multipart()
                    self._logger.debug(f'OMS receives: {msg}')
                    future_results.append(loop.run_in_executor(pool, self._process_zmq_msg, msg))

                for name, b in self._brokers.items():
                    if not b.is_connected and b.is_time_to_reconnect():
                        self._logger.info(f'Try to reconnect broker {name}, retry interval: '
                                          f'{b.reconnect_interval_in_sec} sec...')
                        if not b.is_connecting:
                            loop.run_in_executor(pool, b.connect)
                        else:
                            self._logger.info(f'Broker {name} is already trying to reconnect')
                    elif b.is_connected and datetime.now() - last_ping > self.PING_INTERVAL:
                        last_ping = datetime.now()
                        loop.run_in_executor(pool, b.ping)

                sessions = self._sessions
                for sid in list(sessions.keys()):
                    session = sessions[sid]
                    if session.is_expired:
                        self._logger.warning(f'Lost heartbeat from client {sid}, {session}, disconnecting...')
                        sessions.pop(sid)
                    else:
                        if session.is_heartbeat_due:
                            future = loop.run_in_executor(pool, self._send_heartbeat, sid, session)
                            future_results.append(future)

                        if session.require_stop_check():
                            loop.run_in_executor(pool, self._check_positions, session)


    def publish_msg(self, msg: list):
        self._pending_messages.append(msg)

    def _check_positions(self, session):
        errMsg = session.validate_stop_orders()
        if errMsg:
            # Choose not to send error msg to client
            # because smartquant only handle order reject.
            self._logger.warning(errMsg)

    @property
    def ledger(self):
        return self._ledger

    @staticmethod
    def _build_error_reply(code: ErrorCode, msg: str):
        reply = OmsMessageError()
        reply.error_code = code
        reply.message = msg
        return reply

    @staticmethod
    def _generate_request_id():
        t = datetime.now()
        init_str = f"{t.strftime('%y%m%d%H%M%S')}00000"
        return int(init_str)

    @staticmethod
    def _get_direction(is_buy: bool):
        return 1 if is_buy else -1

    def _lookup_session_by_order_id(self, broker_order_id: int):
        for _, s in self._sessions.items():
            if s.is_own_order(broker_order_id):
                return s
        return None

    def _place_stop(self, session_id: str, market: Market, symbol: str, is_buy: bool, quantity: int, price: float,
                    portfolio: str, strategy: str, parent_order_id: int, comment: Dict[str, str] = None,
                    session: ClientSession = None):
        args = locals()
        if not self.is_ready():
            self._logger.warning(f'OMS is not ready, order {args} was not sent')
            return

        broker_id, broker_order_id = self.place_order(market, symbol, OrderType.STP, is_buy, quantity, price)

        if broker_order_id is not None:
            self.ledger.insert_order(session_id, 0, parent_order_id, broker_id, broker_order_id, market, symbol,
                                     OrderType.STP, is_buy, quantity, price, portfolio, Action.STOP_LOSS, strategy,
                                     None, comment)

        if session is not None:
            session.notify_unsolicited_order(broker_order_id)

    def _process_zmq_msg(self, msg):
        self._logger.debug(f'Worker receives: {msg}')

        src_id = msg[0]
        payload = msg[1]
        try:
            message = OmsMessage.from_json(payload)
            self._logger.debug(f'Decoded: {message}')

            if src_id in self._sessions:
                session = self._sessions[src_id]
            else:
                if message.msg_type == MsgType.INIT:
                    session_id = message.session_id

                    for s in self._sessions.values():
                        if s.id == session_id:
                            reply = self._build_error_reply(ErrorCode.DUPLICATED_SESSION_ID,
                                                            f'An OMS client with same session ID {session_id} has '
                                                            f'logged in already.')
                            msg[1] = reply.to_bytes()
                            return msg

                    session = ClientSession(session_id, src_id, self)
                    self._sessions[src_id] = session
                    self._logger.info(f'Create session {session}, with source ID {src_id}')
                else:
                    if message.msg_type != MsgType.HEARTBEAT:
                        reply = self._build_error_reply(ErrorCode.NOT_LOGGED_IN,
                                                        f'No OMS client with source ID {src_id} is logged in')
                        msg[1] = reply.to_bytes()
                        self._logger.info(f'Message from non-logged in connection: {message.msg_type}, {reply.message}')
                        return msg
                    else:
                        self._logger.info(f'Ignore heartbeat from non-logged in connection: {message}')
                        return None

            reply = session.process(message)
            if reply is not None:
                msg[1] = reply.to_bytes()
                return msg
        except ValueError as e:
            self._logger.exception(f'Error occurred when decoding client message: {payload}', e)

        return None

    def _reconcile_instruments(self) -> List[Tuple[str, Instrument]]:
        """
        Reconcile the instrument data from JSON with those already stored in database

        :return:
        """
        db_insts = self._ledger.query_instruments()

        roll_list = []
        for instrument in InstrumentRepository().instruments:
            is_new_instrument = True
            for dbi in db_insts:
                if (dbi[TableInstrument.MARKET] == instrument.market.value and
                        dbi[TableInstrument.SYMBOL] == instrument.symbol):
                    is_new_instrument = False
                    if (dbi[TableInstrument.CODE] != instrument.front_month.symbol and
                            dbi[TableInstrument.EXPIRY] < instrument.front_month.expiry.date()):
                        self._logger.info(f'Contract roll detected, symbol: {instrument.symbol}, from '
                                          f'{dbi[TableInstrument.CODE]}|{dbi[TableInstrument.EXPIRY]}, to '
                                          f'{instrument.front_month.symbol}|{instrument.front_month.expiry}')
                        roll_list.append((dbi[TableInstrument.CODE], instrument))

                        self._logger.info(
                            f'Instrument {instrument}, update front month contract to {instrument.front_month.symbol}')
                        self._ledger.update_instrument(instrument.market, instrument.symbol,
                                                       instrument.front_month.symbol,
                                                       instrument.front_month.expiry)
                    else:
                        continue

            if is_new_instrument:
                self._logger.info(
                    f'Instrument {instrument}, {instrument.front_month.symbol} is not found in OMS before, adding it')
                self._ledger.update_instrument(instrument.market, instrument.symbol, instrument.front_month.symbol,
                                               instrument.front_month.expiry)
        return roll_list

    def _roll_contracts(self):
        self._logger.info("Check if OMS needs to roll any contract")

        roll_list = self._reconcile_instruments()

        if len(roll_list) > 0:
            self._logger.info("Contract roll is required, waiting for all broker connections to be ready...")
            self._wait_for_brokers(timeout=timedelta(seconds=30))

            for broker_name, b in self._brokers.items():
                if not b.is_connected:
                    self._logger.info(f'The broker {broker_name} is not connected yet, skip contract roll this time')
                    return

            self._logger.info("All brokers are connected")

            # TODO: figure out which broker hold the position, assume only IB now
            for roll in roll_list:
                last_month_code = roll[0]
                instrument = roll[1]

                self._logger.info(f'Roll contract {instrument.symbol}...')

                now_in_exch_time = datetime.now(tz=instrument.timezone)
                roll_instruction = instrument.roll_instruction
                if (roll_instruction and
                    roll_instruction.roll_on_next_start and
                    roll_instruction.from_ == last_month_code and
                    roll_instruction.to == instrument.front_month.symbol and
                    roll_instruction.date == now_in_exch_time.date()):
                    expected = roll_instruction.net_position
                    self._logger.info(f'Roll instruction found, from {roll_instruction.from_} to '
                                      f'{roll_instruction.to}, roll on {roll_instruction.date},'
                                      f' offset {roll_instruction.offset}, position {expected}, can carry out rolling')

                    result = self._ledger.query_total_position(instrument.symbol)
                    total_position = result[0][TablePosition.POSITION]
                    assert total_position == expected, f"Expected roll positiion {expected} but was {total_position}"

                    self._roll_one_symbol(instrument, roll_instruction, total_position)
                else:
                    self._logger.info(f'Cannot find any roll instruction to roll from {last_month_code} to '
                                      f'{instrument.front_month.symbol} on {now_in_exch_time.date()}, no rolling '
                                      f'occurred')
        else:
            self._logger.info(f'No contract requires rolling')

    def _roll_one_symbol(self, instrument: Instrument, roll_instruction: RollInstruction, total_position: int) -> None:
        # Roll position if the net position of the same instrument of all strategies is not zero
        if total_position == 0:
            self._logger.info(f'The aggregated position of {instrument.symbol} is 0, no position rolling is required')
        else:
            self._logger.info(f'The aggregated position of {instrument.symbol} is {total_position}, position rolling is required')
            portfolios = self.ledger.query_portfolio()
            portfolio = portfolios[0][TablePortfolio.ID]

            self._roll_orders.clear()
            # Liquidate front month
            is_buy = True if total_position < 0 else False
            self._send_roll_order(instrument.market, instrument.symbol, roll_instruction.from_, is_buy,
                                total_position, portfolio)

            # Establish position with next month
            is_buy = not is_buy
            self._send_roll_order(instrument.market, instrument.symbol, roll_instruction.to, is_buy,
                                total_position, portfolio)

            self._logger.info('Waiting for all roll order to be filled...')
            self._wait_for_roll_orders()
            self._logger.info('All roll orders has been filled')

        # Roll stop orders if strategy has position, even when the net position of an instrument is 0
        self._roll_stop_loss_orders(instrument, roll_instruction)

    def _roll_stop_loss_orders(self, instrument: Instrument, roll_instruction: RollInstruction) -> None:
        positions = self.ledger.query_position(symbol=instrument.symbol)
        for pos in positions:
            strategy = pos[TablePosition.STRATEGY]
            position = pos[TablePosition.POSITION]

            if position == 0:
                self._logger.info(f'The strategy {strategy} has no position, do not need to roll the stop order')
            else:
                """
                Assumptions:
                * Each strategy only has one active stop-loss order
                * Each OMS instance is responsible for rolling all stop-loss found in a DB
                """
                self._logger.info(f'The strategy {strategy} has a position of {position}, roll the stop order')

                orders = self.ledger.query_order(
                    strategy=strategy, symbol=instrument.symbol, order_type=OrderType.STP,
                    action=Action.STOP_LOSS, active_orders_only=True, order_by_last_modified=True)

                if len(orders) == 0:
                    self._logger.warning(f'Strategy {strategy} has {position} position but no active stop order. Skip rolling the stop order')
                    return

                for order in orders:
                    order_id = order[TableOrder.BROKER_ORDER_ID]
                    is_buy = order[TableOrder.IS_BUY]
                    quantity = order[TableOrder.QUANTITY]
                    price = order[TableOrder.PRICE]
                    portfolio = order[TableOrder.PORTFOLIO]
                    strategy = order[TableOrder.STRATEGY]
                    comment = ujson.loads(order[TableOrder.COMMENT]) if order[TableOrder.COMMENT] else None
                    parent_order_id = order[TableOrder.PARENT_ORDER_ID]

                    self._logger.info(f'Remove original stop-loss order: {order_id}')
                    self.get_broker().cancel_order(order_id)

                    price = price + Decimal(roll_instruction.offset)
                    self._logger.info(f'Place new stop-loss order, is_buy: {is_buy}, {quantity}@{price}')
                    broker_id, broker_order_id = self.place_order(
                        instrument.market, instrument.symbol, OrderType.STP, is_buy, quantity, price)
                    self.ledger.insert_order(strategy, 0, parent_order_id, broker_id, broker_order_id,
                                             instrument.market, instrument.symbol, OrderType.STP, is_buy, quantity,
                                             price, portfolio, Action.STOP_LOSS, strategy, None, comment)

    def _send_heartbeat(self, src_id, session: ClientSession):
        payload = session.send_heartbeat()
        msg = [src_id, payload.to_bytes()]
        return msg

    def _send_roll_order(self, market: Market, symbol: str, contract: str, is_buy: bool, quantity: int, portfolio: str) -> None:
        qty = abs(quantity)
        broker_id, broker_order_id = self.place_order(market=market, symbol=contract, order_type=OrderType.MKT,
                                                      is_buy=is_buy, quantity=qty, price=0)
        self.ledger.insert_order(self.STRATEGY_NAME, 0, 0, broker_id, broker_order_id, market, symbol, OrderType.MKT,
                                 is_buy, qty, 0, portfolio, Action.ROLL, self.STRATEGY_NAME, None, None)
        self._roll_orders.add(broker_order_id)

    def _wait_for_brokers(self, timeout=timedelta(seconds=5)) -> None:
        start = datetime.now()
        while datetime.now() - start < timeout:
            for b in self._brokers.values():
                if not b.is_connected:
                    time.sleep(0.5)
                    continue
                break

    def _wait_for_roll_orders(self) -> None:
        while True:
            if len(self._roll_orders) == 0:
                break
            time.sleep(0.5)
