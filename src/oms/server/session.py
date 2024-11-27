import logging
from datetime import datetime, timedelta
from enum import auto
from threading import RLock
from typing import Any, Dict, List

import numpy as np
import ujson

import oms.common.message as m
from gateway_lib import ExecutionUpdate
from smartquant.common.market import Market
from smartquant.common.utils.autoname import AutoName
from smartquant.execution.base import Action, OrderType
from .ledger.statement import TableOrder, TablePortfolio, TablePosition, TablePositionByEntry, TableOperation


class ClientSessionState(AutoName):
    NEW = auto()
    LOGGED_IN = auto()
    DISCONNECTED = auto()


class ClientSession:
    def __init__(self, session_id, src_id, oms):
        self._logger = logging.getLogger(__name__)
        self._state = ClientSessionState.NEW
        self._session_id = session_id
        self._src_id = src_id
        self._account_id = None
        self._next_request_id = None
        self._oms = oms
        self._orders: Dict[Any, int] = dict()
        self._unsolicited_orders: List[int] = []
        self._last_heartbeat_from_client: datetime = None
        self._next_heartbeat: datetime = datetime.now()
        self._lock = RLock()
        self._last_stopcheck = datetime.now()

        # TODO: populate self._orders from ledger

        orders = self._oms.ledger.query_order(session_id=self.id, active_orders_only=True)
        if len(orders) > 0:
            self._logger.info(
                f'Found outstanding order(s) found for session {self.id}, assigning order(s) back to the session')

            for o in orders:
                order_id = o[TableOrder.ORDER_ID]
                broker_order_id = int(o[TableOrder.BROKER_ORDER_ID])
                if order_id == 0:
                    self._unsolicited_orders.append(broker_order_id)
                else:
                    self._orders[order_id] = broker_order_id
                self._logger.info(
                    f'Session [{self.id}], add order: OMS order ID: {order_id}, broker order ID: {broker_order_id}')
        else:
            self._logger.info(f'No out standing order(s) found for session {self.id}')

    def __str__(self):
        return f'Session: {self.id}, Account: {self.account}, Next request ID: {self._next_request_id}'

    def is_own_order(self, broker_order_id: int) -> bool:
        sid = self.find_session_order_id(broker_order_id)
        if sid is None:
            return False
        return True

    def notify_unsolicited_order(self, broker_order_id: int):
        self._unsolicited_orders.append(broker_order_id)

    def place_order(self, session_order_id: int, market: Market, symbol: str,
        is_buy: bool, order_type: OrderType, quantity: int, price: float,
        portfolio: str, action: str, strategy: str, reference: str, comment: Dict[str, str], session_parent_order_id: int = None):
        args = locals()

        if not self._oms.is_ready():
            self.publish_order_rejected(session_order_id, 'Gateway is down')
            return

        if not self._oms.ledger.verify_account_portfolio_strategy(self.account, portfolio, strategy):
            self.publish_order_rejected(session_order_id,
                                        f"Either account: {self.account}/portfolio: {portfolio}/strategy: {strategy} doesn't exist in OMS database")
            return

        # reject incoming order request if its associated constraint is violated
        constraint = comment.get(TableOrder.COMMENT_CONSTRAINT, None)
        if constraint:
            ledger = self._oms._ledger
            try:
                row = ledger.query_position(portfolio_id=portfolio, strategy=strategy, market=market.value, symbol=symbol)[0]
                current = row[TablePosition.POSITION]
                projected = current + (quantity * (1 if is_buy else -1))
                if ((constraint == TableOrder.Constraint.LONG_ONLY and projected < 0)
                    or
                    (constraint == TableOrder.Constraint.SHORT_ONLY and projected > 0)):
                    self.publish_order_rejected(session_order_id,
                        f"Violated '{constraint}' constraint with projected position equals {projected}")
                    return
            except IndexError:
                # new strategy with no position record or misconfig will fallback to here
                pass

        if session_parent_order_id is None:
            session_parent_order_id = session_order_id

        if action == Action.EXIT.value:
            self._pull_stop_orders(portfolio, strategy, market, symbol, quantity, comment)

        good_till = comment.get(TableOrder.COMMENT_GOOD_TILL, "")
        broker_id, broker_order_id = self._oms.place_order(market, symbol, order_type, is_buy, quantity, price, good_till=good_till)

        if broker_order_id is not None:
            self._orders[session_order_id] = broker_order_id
            self._oms.ledger.insert_order(self._session_id, session_order_id, session_parent_order_id, broker_id,
                                          broker_order_id, market, symbol, order_type, is_buy, quantity, price,
                                          portfolio, action, strategy, reference, comment)

            if action == Action.ENTRY.value:
                try:
                    order_ref = comment[TableOrder.COMMENT_ORDER_REFERENCE]
                except KeyError:
                    order_ref = None

                if order_ref:
                    self._logger.info(f'Found order reference in ENTRY order: {order_ref}, adding a row to position '
                                      f'by entry table')
                    self._oms.ledger.insert_position_by_entry(portfolio, strategy, market, symbol, quantity,
                                                              self._session_id, session_order_id, order_ref)
        else:
            self._logger.warning(f'Order: {args} was not sent')

    def place_stop(self, market: Market, symbol: str, is_buy: bool, quantity: int, price: float, portfolio: str,
                   strategy: str, parent_order_id: int, comment: Dict[str, str] = None):
        self.place_order(0, market, symbol, is_buy, OrderType.STP, quantity, price, portfolio, Action.STOP_LOSS,
                         strategy, None, comment, session_parent_order_id=parent_order_id)

    def process(self, message: m.OmsMessage):
        with self._lock:
            if hasattr(message, 'request_id'):
                self._oms.ledger.increment_next_request_id(self._session_id)

            if message.msg_type == m.MsgType.INIT:
                return self.process_req_init(message)
            elif message.msg_type == m.MsgType.NEXT_REQUEST_ID:
                return self.process_req_next_request_id(message)
            elif message.msg_type == m.MsgType.HEARTBEAT:
                self._last_heartbeat_from_client = datetime.now()
                return self.process_req_hearbeat(message)
            else:
                if not self.is_logged_in:
                    return self._build_error_reply(m.ErrorCode.NOT_LOGGED_IN, 'Session is not logged in yet')

                reply = self._check_next_request_id(message.request_id)
                if reply:
                    return reply

                if message.msg_type == m.MsgType.NEW_ORDER:
                    return self.process_req_new_order(message)
                elif message.msg_type == m.MsgType.POSITION:
                    return self.process_req_position(message)
                elif message.msg_type == m.MsgType.HEARTBEAT:
                    return self.process_req_hearbeat(message)
                else:
                    reply = m.OmsMessageError()
                    reply.error_code = m.ErrorCode.SYSTEM_ERROR
                    reply.message = f'Unknown message type {message.msg_type} received'
                    return reply

    def process_req_init(self, message: m.OmsMessageInit):
        ledger = self._oms.ledger
        session_id = message.session_id
        account_id = message.account_id
        if self._state == ClientSessionState.NEW:
            aid, cash, currency = ledger.query_account(account_id)

            if aid is None:
                # Invalidate the session
                self._invalidate()
                return self._build_error_reply(m.ErrorCode.INIT_ERROR, f'Account {account_id} not found in OMS')
            else:
                self._account_id = aid
                self._logger.info(f'Session {self.id} associated with account {self.account}')

            strategies = message.strategies

            for strategy, portfolio in strategies.items():
                if not ledger.verify_account_portfolio_strategy(self.account, portfolio, strategy):
                    self._logger.warning(f'The strategy {strategy} is not found in OMS database. Adding it...')
                    ledger.insert_strategy(strategy)
                    self._logger.warning(f'The strategy {strategy} has been added to OMS database')

                if not ledger.verify_account_portfolio_strategy(self.account, portfolio, strategy):
                    msg = (f"Either account: {account_id}/portfolio: {portfolio}/strategy: {strategy} doesn't exist "
                           f"in OMS database")
                    self._logger.error(msg)
                    self._invalidate()
                    return self._build_error_reply(m.ErrorCode.INIT_ERROR, msg)

            _, next_request_id, ip = ledger.query_session(session_id)
            try:
                self._last_heartbeat_from_client = datetime.now()
                if next_request_id:
                    self._logger.info(f'Found session ID: {session_id}, returning next request ID: {next_request_id}')
                    reply = m.OmsMessageNextRequestId()
                    reply.next_request_id = next_request_id
                else:
                    self._logger.info(f'Received new session ID {session_id}, adding record')
                    ledger.insert_session(session_id)
                    reply = m.OmsMessageNextRequestId()
                    reply.next_request_id = 1

                self._next_request_id = reply.next_request_id
                return reply
            finally:
                self._state = ClientSessionState.LOGGED_IN
        else:
            reply = m.OmsMessageError()
            reply.error_code = m.ErrorCode.ALREADY_LOGGED_IN
            reply.msg = f'Session {self.id} is logged in already'
            return reply

    def process_req_next_request_id(self, message: m.OmsMessageNextRequestId):
        return None

    def process_req_new_order(self, message: m.OmsMessageNewOrder):
        session_order_id = message.request_id

        market = Market[message.market]
        symbol = message.symbol
        is_buy = message.is_buy
        order_type = OrderType[message.order_type]
        quantity = message.quantity
        price = message.price
        portfolio = message.portfolio
        action = message.action
        strategy = message.strategy
        reference = message.reference
        comment = message.comment

        self.place_order(session_order_id, market, symbol, is_buy, order_type, quantity, price, portfolio, action,
                         strategy, reference, comment)
        return None

    def process_req_position(self, message: m.OmsMessagePosition):
        return self._build_position_message(message.request_id)

    def process_req_hearbeat(self, message: m.OmsMessageHeartbeat):
        self._logger.debug(f'Received heartbeat from client: {message}')

    def publish_execution(self, execution: ExecutionUpdate, order: dict):
        self._send_msg(self._build_execution_message(execution, order))

    def publish_order_error(self, order_id: int, msg: str):
        session_order_id = self.find_session_order_id(order_id)
        self._send_msg(self._build_error_reply(m.ErrorCode.ORDER_ERROR, msg, session_order_id))

    def publish_order_rejected(self, order_id: int, msg: str):
        self._send_msg(self._build_error_reply(m.ErrorCode.ORDER_REJECTED, msg, order_id))

    def publish_position(self):
        self._send_msg(self._build_position_message())

    def publish_position_renew(self):
        self._send_msg(self._build_position_message(force_renew=True))

    def publish_next_request_id(self):
        self._send_msg(self._build_next_request_id_message())

    def send_heartbeat(self):
        msg = m.OmsMessageHeartbeat()
        now = datetime.now()
        msg.timestamp = now.isoformat()
        self._next_heartbeat = now + timedelta(seconds=m.Heartbeat.INTERVAL)
        msg.next = self._next_heartbeat.isoformat()
        msg.is_ready = self._oms.is_ready()
        return msg

    def _send_msg(self, msg: m.OmsMessage):
        reply = [self._src_id, msg.to_bytes()]
        self._oms.publish_msg(reply)

    @property
    def account(self):
        return self._account_id

    @property
    def id(self):
        return self._session_id

    @property
    def is_logged_in(self):
        return self._state == ClientSessionState.LOGGED_IN

    @property
    def is_expired(self):
        return m.Heartbeat.is_expired(self._last_heartbeat_from_client)

    @property
    def is_heartbeat_due(self):
        return datetime.now() > self._next_heartbeat

    def _build_error_reply(self, code: m.ErrorCode, msg: str, request_id: int = None):
        self._logger.error(
            f'Return error to client, session: {self.id}, request_id: {request_id}, code: {code}, message: {msg}')
        reply = m.OmsMessageError()
        reply.error_code = code
        reply.message = msg
        reply.session_id = self.id
        if request_id is not None:
            reply.request_id = request_id
        return reply

    def _build_execution_message(self, execution: ExecutionUpdate, order: dict):
        reply = m.OmsMessageExecution()

        comment = None
        try:
            comment = ujson.loads(order[TableOrder.COMMENT])
        except TypeError:
            pass

        msg_execution = m.OmsMessageExecution.ItemExecution()
        msg_execution.order_id = order[TableOrder.ORDER_ID]
        msg_execution.execution_id = execution.exec_id
        msg_execution.execution_time = execution.timestamp.isoformat()
        msg_execution.market = order[TableOrder.MARKET]
        msg_execution.symbol = order[TableOrder.SYMBOL]
        msg_execution.is_buy = bool(order[TableOrder.IS_BUY])
        msg_execution.quantity = execution.filled
        msg_execution.price = execution.avg_price
        msg_execution.remaining_quantity = int(order[TableOrder.QUANTITY]) - execution.cum_qty
        msg_execution.portfolio = order[TableOrder.PORTFOLIO]
        msg_execution.strategy = order[TableOrder.STRATEGY]
        msg_execution.action = order[TableOrder.ACTION]
        msg_execution.reference = order[TableOrder.REFERENCE]
        msg_execution.comment = comment

        reply.items.append(msg_execution)
        return reply

    def _build_position_message(self, request_id: int = None, force_renew: bool = False):
        ledger = self._oms.ledger

        reply = m.OmsMessagePosition()
        if request_id:
            reply.request_id = request_id

        aid, cash, currency = ledger.query_account(self.account)
        reply.account = m.OmsMessagePosition.ItemAccount()
        reply.account.id = self.account
        reply.account.cash = cash
        reply.account.currency = currency

        portfolios = self._oms.ledger.query_portfolio(account_id=self.account)
        for p in portfolios:
            portfolio_id = p[TablePortfolio.ID]

            msg_portfolio = m.OmsMessagePosition.ItemPortfolio()
            msg_portfolio.id = portfolio_id

            positions = ledger.query_position(portfolio_id)
            for pos in positions:
                strategy = pos[TablePosition.STRATEGY]
                if strategy != self._session_id:
                    continue

                msg_position = m.OmsMessagePosition.ItemPosition()
                msg_position.strategy = strategy
                msg_position.market = pos[TablePosition.MARKET]
                msg_position.symbol = pos[TablePosition.SYMBOL]
                msg_position.position = pos[TablePosition.POSITION]
                msg_position.avg_price = pos[TablePosition.AVG_PRICE]
                msg_position.force_renew = force_renew
                msg_portfolio.positions.append(msg_position)

                entry_pos = ledger.query_position_by_entry(portfolio_id, strategy, msg_position.market,
                                                           msg_position.symbol)
                msg_position.positions_by_entry = list()
                for ep in entry_pos:
                    msg_position_by_entry = m.OmsMessagePosition.ItemPositionByEntry()
                    msg_position_by_entry.position = ep[TablePositionByEntry.POSITION]
                    msg_position_by_entry.avg_price = ep[TablePositionByEntry.AVG_PRICE]
                    msg_position_by_entry.state = ep[TablePositionByEntry.STATE]
                    msg_position_by_entry.created = ep[TablePositionByEntry.CREATED]
                    operations = ledger.query_operation(portfolio_id, strategy, ep[TablePositionByEntry.ORDER_REFERENCE])
                    if operations:
                        self._logger.info(f'Found operations from {ep[TablePositionByEntry.ORDER_REFERENCE]}, {str(operations)}')
                        msg_position_by_entry.operations = operations
                    entry_order = m.OmsMessagePosition.ItemOrder()
                    entry_order.order_id = ep[TableOrder.ORDER_ID]
                    entry_order.market = msg_position.market
                    entry_order.symbol = msg_position.symbol
                    entry_order.order_type = ep[TableOrder.TYPE]
                    entry_order.is_buy = ep[TableOrder.IS_BUY]
                    entry_order.quantity = ep[TableOrder.QUANTITY]
                    entry_order.price = ep[TableOrder.PRICE]
                    entry_order.portfolio = portfolio_id
                    entry_order.action = ep[TableOrder.ACTION]
                    entry_order.strategy = strategy
                    entry_order.reference = ep[TableOrder.REFERENCE]
                    try:
                        entry_order.comment = ujson.loads(ep[TableOrder.COMMENT])
                    except TypeError:
                        entry_order.comment = None
                    msg_position_by_entry.order = entry_order

                    msg_position.positions_by_entry.append(msg_position_by_entry)
            reply.account.portfolios.append(msg_portfolio)

        return reply

    def _build_next_request_id_message(self):
        ledger = self._oms.ledger
        _, next_request_id, _ = ledger.query_session(self.id)
        self._logger.info(f'Found session ID: {self.id}, returning next request ID: {next_request_id}')
        reply = m.OmsMessageNextRequestId()
        reply.next_request_id = next_request_id
        return reply

    def _check_next_request_id(self, request_id: int):
        if request_id < self._next_request_id:
            return self._build_error_reply(m.ErrorCode.BAD_REQUEST_ID,
                                           f'Request ID received {request_id} < {self._next_request_id}', request_id)
        return None

    def find_session_order_id(self, broker_order_id: int):
        for sid, bid in self._orders.items():
            if bid == broker_order_id:
                return sid
        if broker_order_id in self._unsolicited_orders:
            return 0
        return None

    def _invalidate(self):
        self._last_heartbeat_from_client = datetime.min

    def _pull_stop_orders(self, portfolio: str, strategy: str, market: Market, symbol: str, quantity: int,
                          comment: Dict[str, str]):
        self._logger.info(f'Remove stop-loss order before sending exit order...')

        if comment is not None:
            order_ref = comment.get(TableOrder.COMMENT_ORDER_REFERENCE)
        else:
            order_ref = None

        order_ref_list = []
        if order_ref is None:
            entry_positions = self._oms.ledger.query_position_by_entry(portfolio_id=portfolio, strategy=strategy,
                                                                       market=str(market), symbol=symbol)
            for p in entry_positions:
                order_ref_list.append(p[TablePositionByEntry.ORDER_REFERENCE])
        else:
            order_ref_list.append(order_ref)

        orders = self._oms.ledger.query_order(portfolio=portfolio, strategy=strategy, order_type=OrderType.STP,
                                              active_orders_only=True, order_by_created=True)

        if len(order_ref_list) == 0:
            # There is no order reference, assume SingleEntry. Remove the most recently created stop-loss order
            if orders:
                o = orders[-1]
                order_id = o[TableOrder.BROKER_ORDER_ID]
                self._logger.info(f'Remove stop-loss order: {order_id}')
                self._oms.get_broker().cancel_order(order_id)
            else:
                self._logger.error(f'Fail to remove stop-loss order: order was missed for {portfolio}/{symbol}/{strategy}')
        else:
            removed = []
            for o in orders:
                try:
                    stp_comment = ujson.loads(o[TableOrder.COMMENT])
                    stp_order_ref = stp_comment.get(TableOrder.COMMENT_ORDER_REFERENCE)
                except (TypeError, KeyError):
                    continue

                for o_ref in order_ref_list:
                    if o_ref == stp_order_ref:
                        order_id = o[TableOrder.BROKER_ORDER_ID]
                        self._logger.info(f'Remove stop-loss order: {order_id}, {o_ref}')
                        self._oms.get_broker().cancel_order(order_id)
                        removed.append(o_ref)

                not_pulled = np.setdiff1d(order_ref_list, removed)
                if len(not_pulled) != 0:
                    self._logger.info(f'OMS did not find any stop-loss order with the following order reference: '
                                      f'{not_pulled} when handling exit')

    def require_stop_check(self):
        return datetime.now() - self._last_stopcheck > timedelta(minutes=5)

    def validate_stop_orders(self):
        # assume all positions must be covered by STP orders.
        self._last_stopcheck = datetime.now()
        strategyName = self._session_id
        ledger = self._oms._ledger
        positions = ledger.query_position(strategy=strategyName)
        for record in positions:
            pos = record[TablePosition.POSITION]
            portfolio = record[TablePosition.PORTFOLIO_ID]
            if pos != 0:
                # assume strategy use STP order solely to control risk
                orders = ledger.query_order(
                    portfolio=portfolio,
                    session_id=strategyName,
                    order_type=OrderType.STP,
                    active_orders_only=True)
                stpQty = 0
                for order in orders:
                    # assume strategy trades the same symbol
                    direction = -1 if order[TableOrder.IS_BUY] == 1 else 1
                    stpQty += int(order[TableOrder.QUANTITY]) * direction
                if pos != stpQty:
                    return f"Stop order check failed for strategy '{strategyName}'. Strategy position is {pos} but the total STP order quantity is {-stpQty}"
        return ""