import copy
import logging
from collections import OrderedDict
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Dict

import mysql.connector

from oms.common.config import CFG_MYSQL
from smartquant.execution.base import Action, OrderState, OrderType
from .statement import TableAccount, TableSession, Statement


class DbMySql:
    N_RETRY = 5
    RETRY_DELAY = 2

    def __init__(self, config: OrderedDict):
        self._logger = logging.getLogger(__name__)
        cfg = copy.copy(config[CFG_MYSQL])
        self._logger.info(f'Connect to MySQL database with configuration: {cfg}')
        self._cnx = mysql.connector.connect(**cfg)
        self._lock = RLock()

    def close(self):
        self._cnx.close()

    def get_cursor(self):
        self._cnx.ping(True, self.N_RETRY, self.RETRY_DELAY)
        return self._cnx.cursor(dictionary=True)

    def increment_next_request_id(self, session_id: str):
        stmt = Statement.build_stmt_session_increment_next_request_id(session_id)
        self._exec_stmt(stmt)

    def insert_session(self, session_id: str):
        stmt = Statement.build_stmt_session_insert(session_id, 'dummy')
        self._exec_stmt(stmt)

    def insert_execution(self, broker_id: str, broker_order_id: str, broker_execution_id: str, gateway_order_id: str,
                         is_buy: bool, symbol: str, quantity: int, price: float, leave_quantity: int, commission: float, currency: str, execution_datetime: datetime):
        stmt = Statement.build_stmt_execution_insert(broker_id, broker_order_id, broker_execution_id, gateway_order_id,
                                                     is_buy, symbol, quantity, price, leave_quantity, commission, currency, execution_datetime)
        self._exec_stmt(stmt)

    def insert_order(self, session_id: str, order_id: int, parent_order_id: int, broker_id: str, broker_order_id: str,
                     market: str, symbol: str, order_type: OrderType, is_buy: bool, quantity: int, price: float,
                     portfolio: str, action: str, strategy: str, reference: str, comment: Dict[str, Any]):
        if price is None and order_type == OrderType.MKT:
            price = 0

        stmt = Statement.build_stmt_order_insert(session_id, order_id, parent_order_id, broker_id, broker_order_id,
                                                 market, symbol, order_type, is_buy, quantity, price, 'none', portfolio,
                                                 action, strategy, reference, comment)
        self._exec_stmt(stmt)

    def insert_position_by_entry(self, portfolio_id: str, strategy: str, market: str, symbol: str, position: int,
                                 session_id: str, order_id: int, order_reference: str, avg_price: float=0.0, state: str='PENDING'):
        stmt = Statement.build_stmt_position_by_entry_insert(portfolio_id, strategy, market, symbol, position, avg_price,
                                                             session_id, order_id, state, order_reference)
        self._exec_stmt(stmt)

    def update_position_by_entry(self, session_id: str = None, order_id: int = None, portfolio_id: str = None,
                                 strategy: str = None, order_reference: str = None, avg_price: float = None,
                                 state: str = None, position: int = None):
        stmt = Statement.build_stmt_position_by_entry_update(session_id=session_id, order_id=order_id,
                                                             portfolio_id=portfolio_id, strategy=strategy,
                                                             order_reference=order_reference, avg_price=avg_price,
                                                             state=state, position=position)
        self._exec_stmt(stmt)

    def delete_position_by_entry(self, session_id: str, order_id: int):
        stmt = f"delete from oms.position_by_entry where session_id = '{session_id}' and order_id = {order_id}"
        self._exec_stmt(stmt)

    def insert_operation(self, portfolio_id: str, strategy: str, action: str, position: int, order_reference: str,
                         price: float = None, identity: str = None):
        stmt = Statement.build_stmt_operation_insert(portfolio_id, strategy, action, position, order_reference, price, identity)
        self._exec_stmt(stmt)

    def insert_strategy(self, strategy: str):
        stmt = Statement.build_stmt_strategy_insert(strategy)
        self._exec_stmt(stmt)

    def query_account(self, account_id: str):
        stmt = Statement.build_stmt_account_select_by_id(account_id)
        result = self._exec_query(stmt)
        if len(result) == 1:
            return result[0][TableAccount.ID], result[0][TableAccount.CASH], result[0][TableAccount.CURRENCY]
        return None, None, None

    def verify_account_portfolio_strategy(self, account_id: str, portfolio_id: str, strategy: str):
        stmt = Statement.build_stmt_find_account_portfolio_strategy(account_id, portfolio_id, strategy)
        result = self._exec_query(stmt)
        if len(result) > 0:
            return True
        return False

    def query_executions(self, broker_id: str, broker_execution_id: str = None, lookback: timedelta = None):
        if lookback:
            last_time = datetime.now() - lookback
        else:
            last_time = None
        stmt = Statement.build_stmt_execution_select_by_broker_id_and_date(broker_id, broker_execution_id, last_time)
        return self._exec_query(stmt)

    def query_instruments(self):
        stmt = Statement.build_stmt_instrument_select()
        return self._exec_query(stmt)

    def query_order(self, broker_id: str = None, session_id: str = None, order_id: int = None,
                    broker_order_id: str = None, symbol: str = None, action: Action = None, portfolio: str = None,
                    strategy: str = None, order_type: OrderType = None, active_orders_only: bool = False,
                    order_by_last_modified=False, order_by_created=False):
        stmt = Statement.build_stmt_order_select(broker_id, session_id, order_id, broker_order_id, symbol, action,
                                                 portfolio, strategy,
                                                 order_type.value if order_type is not None else None,
                                                 active_orders_only, order_by_last_modified, order_by_created)
        return self._exec_query(stmt)

    def query_portfolio(self, portfolio_id: str = None, account_id: str = None):
        stmt = Statement.build_stmt_portfolio_select_by_id_and_account_id(portfolio_id, account_id)
        return self._exec_query(stmt)

    def query_position(self, portfolio_id: str = None, strategy: str = None, market: str = None, symbol: str = None):
        stmt = Statement.build_stmt_position_select(portfolio_id, strategy, market, symbol)
        return self._exec_query(stmt)

    def query_position_by_entry(self, portfolio_id: str = None, strategy: str = None, market: str = None,
                                symbol: str = None):
        stmt = Statement.build_stmt_position_by_entry_select_by_position(portfolio_id, strategy, market, symbol)
        return self._exec_query(stmt)

    def query_operation(self, portfolio_id: str, strategy: str, order_reference: str):
        stmt = Statement.build_stmt_operation_select(portfolio_id, strategy, order_reference)
        return self._exec_query(stmt)

    def query_session(self, session_id: str):
        stmt = Statement.build_stmt_session_select_by_id(session_id)
        results = self._exec_query(stmt)
        if len(results) == 1:
            row = results[0]
            return row[TableSession.ID], row[TableSession.NEXT_REQUEST_ID], row[TableSession.IP]
        return None, None, None

    def query_total_position(self, symbol: str):
        stmt = Statement.build_stmt_position_sum(symbol)
        return self._exec_query(stmt)

    def update_instrument(self, market: str, symbol: str, code: str, expiry: datetime):
        stmt = Statement.build_stmt_instrument_insert_or_update(market, symbol, code, expiry)
        self._exec_stmt(stmt)

    def update_order(self, broker_id: str, broker_order_id: str, quantity: int = None, price: float = None,
                     remaining_quantity: int = None, filled_quantity: int = None, state: OrderState = None,
                     action: Action = None):
        stmt = Statement.build_stmt_order_update(broker_id, broker_order_id, quantity, price, remaining_quantity,
                                                 filled_quantity, state, action)
        self._exec_stmt(stmt)

    def update_position(self, portfolio_id: str, strategy: str, market: str, symbol: str, position: int,
                        avg_price: float = None):
        stmt = Statement.build_stmt_position_insert_or_update(portfolio_id, strategy, market, symbol, position, avg_price)
        self._exec_stmt(stmt)

    def _exec_query(self, stmt: str):
        with self._lock:
            cursor = self.get_cursor()
            try:
                self._exec_stmt(stmt, cursor, False)
                return cursor.fetchall()
            finally:
                cursor.close()

    def _exec_stmt(self, stmt: str, cursor=None, commit: bool = True):
        with self._lock:
            self._logger.info(f'Execute: {stmt}')
            local_cursor = cursor if cursor else self.get_cursor()
            try:
                local_cursor.execute(stmt)
                if commit:
                    self._cnx.commit()
            except mysql.connector.Error as e:
                self._logger.exception(f'MySQL exception when executing: {stmt}', e)
                raise e
            finally:
                if not cursor:
                    local_cursor.close()
