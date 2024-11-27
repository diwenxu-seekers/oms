from datetime import datetime
from enum import Enum
from numbers import Number
from typing import Any, Dict, List, Tuple

import ujson

from smartquant.execution.base import Action, OrderState


class AllTables:
    CREATED = 'created'
    LAST_MODIFIED = 'last_modified'


class TableAccount:
    table_name = 'account'
    ID = 'id'
    CASH = 'cash'
    CURRENCY = 'currency'


class TableBroker:
    table_name = 'broker'
    ID = 'id'
    DESCRIPTION = 'description'


class TableExecution:
    table_name = 'execution'
    BROKER_ID = 'broker_id'
    BROKER_EXECUTION_ID = 'broker_execution_id'
    BROKER_ORDER_ID = 'broker_order_id'
    GATEWAY_ORDER_ID = 'gateway_order_id'
    IS_BUY = 'is_buy'
    SYMBOL = 'contract'
    QUANTITY = 'quantity'
    PRICE = 'price'
    LEAVE_QUANTITY = 'leave_quantity'
    COMMISSION = 'commission'
    CURRENCY = 'currency'
    EXECUTION_DATETIME = 'execution_datetime'


class TableInstrument:
    table_name = 'instrument'
    MARKET = 'market'
    SYMBOL = 'symbol'
    CODE = 'code'
    EXPIRY = 'expiry'


class TableMarket:
    table_name = 'market'
    MARKET = 'market'


class TableOrder:

    class Constraint:
        LONG_ONLY = 'long-only'
        SHORT_ONLY = 'short-only'

    table_name = 'order_'
    ORDER_ID = 'order_id'
    PARENT_ORDER_ID = 'parent_order_id'
    BROKER_ID = 'broker_id'
    BROKER_ORDER_ID = 'broker_order_id'
    SESSION_ID = 'session_id'
    MARKET = 'market'
    SYMBOL = 'symbol'
    TYPE = 'type'
    IS_BUY = 'is_buy'
    QUANTITY = 'quantity'
    PRICE = 'price'
    STATE = 'state'
    FILLED_QUANTITY = 'filled_quantity'
    REMAINING_QUANTITY = 'remaining_quantity'
    QUALIFIER = 'qualifier'
    PORTFOLIO = 'portfolio'
    ACTION = 'action'
    STRATEGY = 'strategy'
    REFERENCE = 'reference'
    COMMENT = 'comment'
    COMMENT_ATTACHMENT = 'attachment'
    COMMENT_CONSTRAINT = 'constraint'
    COMMENT_COST = 'cost'
    COMMENT_CUSTOMIZED_QUANTITY = 'customized_quantity'
    COMMENT_GOOD_TILL = 'good_till'
    COMMENT_ORDER_REFERENCE = 'order_reference'
    COMMENT_PATTERN_NAME = 'pattern_name'
    COMMENT_TIMESTAMP = 'exchange_timestamp'
    COMMENT_STOP_LOSS_ABSOLUTE = 'stop_loss_absolute'
    COMMENT_STOP_LOSS_OFFSET = 'stop_loss_offset'
    COMMENT_RISK_FACTOR = 'risk_factor'
    ACTIVE_STATES = ['NEW', 'PENDING', 'ACTIVE', 'PARTICALLY_FILLED']


class TablePortfolio:
    table_name = 'portfolio'
    ID = 'id'
    ACCOUNT_ID = 'account_id'


class TablePosition:
    table_name = 'position'
    PORTFOLIO_ID = 'portfolio_id'
    STRATEGY = 'strategy'
    MARKET = 'market'
    SYMBOL = 'symbol'
    POSITION = 'position'
    AVG_PRICE = 'avg_price'


class TablePositionByEntry:
    table_name = 'position_by_entry'
    PORTFOLIO_ID = 'portfolio_id'
    STRATEGY = 'strategy'
    MARKET = 'market'
    SYMBOL = 'symbol'
    POSITION = 'position'
    AVG_PRICE = 'avg_price'
    SESSION_ID = 'session_id'
    ORDER_ID = 'order_id'
    STATE = 'state'
    ORDER_REFERENCE = 'order_reference'
    CREATED = 'created'
    STATE_PENDING = 'PENDING'
    STATE_FULLY_FILLED = 'FULLY_FILLED'
    STATE_EXITED = 'EXITED'


class TableOperation:
    table_name = 'operation'
    PORTFOLIO_ID = 'portfolio_id'
    STRATEGY = 'strategy'
    ACTION = 'action'
    POSITION = 'position'
    PRICE = 'price'
    ORDER_REFERENCE = 'order_reference'
    IDENTITY = 'identity'
    CREATED = 'created'


class TableSession:
    table_name = 'session'
    ID = 'id'
    NEXT_REQUEST_ID = 'next_request_id'
    IP = 'ip'


class TableStrategy:
    table_name = 'strategy'
    ID = 'id'
    DESCRIPTION = 'description'


class Statement:
    CONDITION_AND = ' and '

    @staticmethod
    def _build_insert_stmt(cols: List[str], table: str, values: List[str], ignore: bool = False):
        insert_keyword = 'insert'
        if ignore:
            insert_keyword = 'insert ignore'
        return f'{insert_keyword} into {table} ({",".join(cols)}) values ({",".join(Statement._to_insert_value(v) for v in values)})'

    @staticmethod
    def _build_select_stmt(cols: List[str], table: str, condition: bool = True):
        return f'select {",".join(cols)} from {table} {"where" if condition else ""} '

    @staticmethod
    def _build_update_stmt(table: str, cols: List[str], values: list):
        set_values = ",".join(f"{c}={Statement._to_insert_value(v)}" for c, v in zip(cols, values))
        return f'update {table} set {set_values} '

    @staticmethod
    def _build_simple_where_clause(items: List[Tuple[str, str]]):
        where_values = " and ".join(f"{c}={Statement._to_insert_value(v)}" for c, v in items)
        return f'where {where_values}'

    @staticmethod
    def _to_insert_value(v) -> str:
        if v is None:
            return 'null'
        elif isinstance(v, Number):
            return str(v)
        elif isinstance(v, str):
            return f"'{v}'"
        elif isinstance(v, datetime):
            return f"'{v.replace(tzinfo=None)}'"
        elif isinstance(v, Enum):
            return f"'{v.value}'"
        return str(v)

    @staticmethod
    def build_stmt_account_select_by_id(account_id: str):
        stmt = Statement._build_select_stmt([TableAccount.ID, TableAccount.CASH, TableAccount.CURRENCY],
                                            TableAccount.table_name)
        conditions = f"{TableAccount.ID}='{account_id}'"
        return f"{stmt}{conditions}"

    @staticmethod
    def build_stmt_execution_select_by_broker_id_and_date(broker_id: str, broker_execution_id: str = None,
                                                          execution_datetime: datetime = None) -> str:
        stmt = Statement._build_select_stmt(
            [TableExecution.BROKER_ID, TableExecution.BROKER_ORDER_ID, TableExecution.BROKER_EXECUTION_ID,
             TableExecution.GATEWAY_ORDER_ID, TableExecution.IS_BUY, TableExecution.QUANTITY, TableExecution.PRICE,
             TableExecution.LEAVE_QUANTITY, TableExecution.EXECUTION_DATETIME], TableExecution.table_name)
        conditions = f"{TableExecution.BROKER_ID}='{broker_id}'"
        if broker_execution_id is not None:
            conditions += f" and {TableExecution.BROKER_EXECUTION_ID}='{broker_execution_id}'"
        if execution_datetime is not None:
            conditions += f" and {TableExecution.EXECUTION_DATETIME}>='{execution_datetime}'"
        return f"{stmt}{conditions}"

    @staticmethod
    def build_stmt_instrument_select():
        stmt = Statement._build_select_stmt(
            [TableInstrument.MARKET, TableInstrument.SYMBOL, TableInstrument.CODE, TableInstrument.EXPIRY],
            TableInstrument.table_name, False)
        return stmt

    @staticmethod
    def build_stmt_order_select(broker_id: str = None, session_id: str = None, order_id: int = None,
                                broker_order_id: str = None, symbol: str = None, action: Action = None,
                                portfolio: str = None, strategy: str = None, order_type: str = None,
                                active_orders_only: bool = False, order_by_last_modified: bool = False,
                                order_by_created: bool = False):
        stmt = Statement._build_select_stmt(
            [TableOrder.SESSION_ID, TableOrder.ORDER_ID, TableOrder.PARENT_ORDER_ID, TableOrder.BROKER_ID,
             TableOrder.BROKER_ORDER_ID, TableOrder.MARKET, TableOrder.SYMBOL, TableOrder.TYPE, TableOrder.IS_BUY,
             TableOrder.QUANTITY, TableOrder.PRICE, TableOrder.STATE, TableOrder.QUALIFIER, TableOrder.PORTFOLIO,
             TableOrder.ACTION, TableOrder.STRATEGY, TableOrder.REFERENCE, TableOrder.COMMENT,
             TableOrder.FILLED_QUANTITY, TableOrder.REMAINING_QUANTITY], TableOrder.table_name)
        conditions = ''
        if broker_id is not None:
            conditions += f"{TableOrder.BROKER_ID}='{broker_id}'"
        if session_id is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.SESSION_ID}='{session_id}'")
        if order_id is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.ORDER_ID}={order_id}")
        if broker_order_id is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.BROKER_ORDER_ID}='{broker_order_id}'")
        if symbol is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.SYMBOL}='{symbol}'")
        if action is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.ACTION}='{action.value}'")
        if portfolio is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.PORTFOLIO}='{portfolio}'")
        if strategy is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.STRATEGY}='{strategy}'")
        if order_type is not None:
            conditions += Statement._append_condition(conditions, f"{TableOrder.TYPE}='{order_type}'")
        if active_orders_only:
            conditions += Statement._append_condition(conditions,
                                                      f"{TableOrder.STATE} in ({','.join(map(Statement._to_insert_value, TableOrder.ACTIVE_STATES))})")

        order_by = ''
        if order_by_last_modified:
            order_by += f" order by {AllTables.LAST_MODIFIED} desc"
        elif order_by_created:
            order_by += f" order by {AllTables.CREATED}"

        return f"{stmt}{conditions}{order_by}"

    @staticmethod
    def build_stmt_portfolio_select_by_id_and_account_id(portfolio_id: str = None, account_id: str = None):
        condition = False if portfolio_id is None and account_id is None else True
        stmt = Statement._build_select_stmt([TablePortfolio.ID, TablePortfolio.ACCOUNT_ID], TablePortfolio.table_name,
                                            condition=condition)
        conditions = ''
        if portfolio_id is not None:
            conditions += f"{TablePortfolio.ID}='{portfolio_id}'"
        if account_id is not None:
            conditions += Statement._append_condition(conditions, f"{TablePortfolio.ACCOUNT_ID}='{account_id}'")
        return f"{stmt}{conditions}"

    @staticmethod
    def build_stmt_position_select(portfolio_id: str = None, strategy: str = None, market: str = None,
                                   symbol: str = None):
        stmt = Statement._build_select_stmt([TablePosition.PORTFOLIO_ID, TablePosition.STRATEGY, TablePosition.MARKET,
                                             TablePosition.SYMBOL, TablePosition.POSITION, TablePosition.AVG_PRICE],
                                            TablePosition.table_name)
        conditions = ''
        if portfolio_id is not None:
            conditions += f"{TablePosition.PORTFOLIO_ID}='{portfolio_id}'"
        if strategy is not None:
            conditions += Statement._append_condition(conditions, f"{TablePosition.STRATEGY}='{strategy}'")
        if market is not None:
            conditions += Statement._append_condition(conditions, f"{TablePosition.MARKET}='{market}'")
        if symbol is not None:
            conditions += Statement._append_condition(conditions, f"{TablePosition.SYMBOL}='{symbol}'")
        return f"{stmt}{conditions}"

    @staticmethod
    def build_stmt_operation_select(portfolio_id: str = None, strategy: str = None, order_reference: str = None):
        stmt = Statement._build_select_stmt([TableOperation.CREATED, TableOperation.ACTION, TableOperation.POSITION, TableOperation.PRICE, TableOperation.IDENTITY],
                                            TableOperation.table_name)
        conditions = ''
        if portfolio_id is not None:
            conditions += f"{TableOperation.PORTFOLIO_ID}='{portfolio_id}'"
        if strategy is not None:
            conditions += Statement._append_condition(conditions, f"{TableOperation.STRATEGY}='{strategy}'")
        if order_reference is not None:
            conditions += Statement._append_condition(conditions, f"{TableOperation.ORDER_REFERENCE}='{order_reference}'")
        return f"{stmt}{conditions}"

    @staticmethod
    def build_stmt_position_sum(symbol: str):
        stmt = Statement._build_select_stmt(
            [TablePosition.SYMBOL, f'sum({TablePosition.POSITION}) as {TablePosition.POSITION}'],
            TablePosition.table_name)
        return f"{stmt}{TablePosition.SYMBOL}='{symbol}'"

    @staticmethod
    def build_stmt_session_select_by_id(session_id: str) -> str:
        stmt = Statement._build_select_stmt([TableSession.ID, TableSession.NEXT_REQUEST_ID, TableSession.IP],
                                            TableSession.table_name)
        return f"{stmt}{TableSession.ID}='{session_id}'"

    @staticmethod
    def build_stmt_find_account_portfolio_strategy(account: str, portfolio: str, strategy: str) -> str:
        return (f"select a.id, p.id, s.id from account as a inner join portfolio as p inner join "
                f"strategy as s on a.id=p.account_id where a.id='{account}' and p.id='{portfolio}' and "
                f"s.id='{strategy}'")

    @staticmethod
    def build_stmt_execution_insert(broker_id: str, broker_order_id: str, broker_execution_id: str,
                                    gateway_order_id: str, is_buy: bool, symbol: str, quantity: int, price: float,
                                    leave_quantity: int, commission: float, currency: str, execution_datetime: datetime) -> str:
        return Statement._build_insert_stmt(
            [TableExecution.BROKER_ID, TableExecution.BROKER_ORDER_ID, TableExecution.BROKER_EXECUTION_ID,
             TableExecution.GATEWAY_ORDER_ID, TableExecution.IS_BUY, TableExecution.SYMBOL, TableExecution.QUANTITY, TableExecution.PRICE,
             TableExecution.LEAVE_QUANTITY, TableExecution.COMMISSION, TableExecution.CURRENCY, TableExecution.EXECUTION_DATETIME], TableExecution.table_name,
            [broker_id, broker_order_id, broker_execution_id, gateway_order_id, is_buy, symbol, quantity, price,
             leave_quantity, commission, currency, execution_datetime])

    @staticmethod
    def build_stmt_instrument_insert_or_update(market: str, symbol: str, code: str, expiry: datetime):
        stmt = Statement._build_insert_stmt(
            [TableInstrument.MARKET, TableInstrument.SYMBOL, TableInstrument.CODE, TableInstrument.EXPIRY],
            TableInstrument.table_name, [market, symbol, code, expiry])
        stmt += f" on duplicate key update {TableInstrument.CODE}='{code}', {TableInstrument.EXPIRY}='{expiry}'"
        return stmt

    @staticmethod
    def build_stmt_order_insert(session_id: str, order_id: int, parent_order_id: int, broker_id: str,
                                broker_order_id: str, market: str, symbol: str, type_: str, is_buy: bool, quantity: int,
                                price: float, qualifier, portfolio: str, action: str, strategy: str, reference: str,
                                comment: Dict[str, Any]):
        comment_serialized = None
        if comment is not None:
            comment_serialized = ujson.dumps(comment)
        order_state = OrderState.NEW.value.lower()

        return Statement._build_insert_stmt(
            [TableOrder.SESSION_ID, TableOrder.ORDER_ID, TableOrder.PARENT_ORDER_ID, TableOrder.BROKER_ID,
             TableOrder.BROKER_ORDER_ID, TableOrder.MARKET, TableOrder.SYMBOL, TableOrder.TYPE, TableOrder.IS_BUY,
             TableOrder.QUANTITY, TableOrder.PRICE, TableOrder.STATE, TableOrder.QUALIFIER, TableOrder.PORTFOLIO,
             TableOrder.ACTION, TableOrder.STRATEGY, TableOrder.REFERENCE, TableOrder.COMMENT], TableOrder.table_name,
            [session_id, order_id, parent_order_id, broker_id, broker_order_id, market, symbol, type_, is_buy, quantity,
             price, order_state, qualifier, portfolio, action, strategy, reference, comment_serialized])

    @staticmethod
    def build_stmt_order_update(broker_id: str, broker_order_id: str, quantity: int = None, price: float = None,
                                remaining_quantity: int = None, filled_quantity: int = None, state: OrderState = None,
                                action: Action = None):
        cols = []
        values = []

        Statement._append_value(cols, values, TableOrder.QUANTITY, quantity)
        Statement._append_value(cols, values, TableOrder.PRICE, price)
        Statement._append_value(cols, values, TableOrder.REMAINING_QUANTITY, remaining_quantity)
        Statement._append_value(cols, values, TableOrder.FILLED_QUANTITY, filled_quantity)
        Statement._append_value(cols, values, TableOrder.STATE, state)
        Statement._append_value(cols, values, TableOrder.ACTION, action)

        stmt = Statement._build_update_stmt(TableOrder.table_name, cols, values)
        stmt += f"where {TableOrder.BROKER_ID}='{broker_id}' and {TableOrder.BROKER_ORDER_ID}='{broker_order_id}'"
        return stmt

    @staticmethod
    def build_stmt_position_insert_or_update(portfolio_id: str, strategy: str, market: str, symbol: str, position: int,
                                             avg_price: float = None):
        if avg_price:
            stmt = Statement._build_insert_stmt(
                [TablePosition.PORTFOLIO_ID, TablePosition.STRATEGY, TablePosition.MARKET, TablePosition.SYMBOL,
                 TablePosition.POSITION, TablePosition.AVG_PRICE], TablePosition.table_name,
                [portfolio_id, strategy, market, symbol, position, avg_price])
            stmt += f" on duplicate key update {TablePosition.POSITION}={TablePosition.POSITION}+{position}, {TablePosition.AVG_PRICE}={avg_price}"
        else:
            stmt = Statement._build_insert_stmt(
                [TablePosition.PORTFOLIO_ID, TablePosition.STRATEGY, TablePosition.MARKET, TablePosition.SYMBOL,
                 TablePosition.POSITION], TablePosition.table_name,
                [portfolio_id, strategy, market, symbol, 0])
            stmt += f" on duplicate key update {TablePosition.POSITION}={TablePosition.POSITION}+{position}"
        return stmt

    @staticmethod
    def build_stmt_position_update(portfolio_id: str, strategy: str, position: int, avg_price: float = None):
        cols = []
        values = []
        Statement._append_value(cols, values, TablePosition.POSITION, position)
        Statement._append_value(cols, values, TablePosition.AVG_PRICE, avg_price)
        stmt = Statement._build_update_stmt(TablePosition.table_name, cols, values)
        stmt += f"where {TablePosition.PORTFOLIO_ID}='{portfolio_id}' and {TablePosition.STRATEGY}='{strategy}'"
        return stmt

    @staticmethod
    def build_stmt_position_by_entry_insert(portfolio_id: str, strategy: str, market: str, symbol: str, position: int,
                                            avg_price: float, session_id: str, order_id: int, state: str,
                                            order_reference: str):
        stmt = Statement._build_insert_stmt(
            [TablePositionByEntry.PORTFOLIO_ID, TablePositionByEntry.STRATEGY, TablePositionByEntry.MARKET,
             TablePositionByEntry.SYMBOL, TablePositionByEntry.POSITION, TablePositionByEntry.AVG_PRICE,
             TablePositionByEntry.SESSION_ID, TablePositionByEntry.ORDER_ID, TablePositionByEntry.STATE,
             TablePositionByEntry.ORDER_REFERENCE],
            TablePositionByEntry.table_name,
            [portfolio_id, strategy, market, symbol, position, avg_price, session_id, order_id, state, order_reference])
        return stmt

    @staticmethod
    def build_stmt_operation_insert(portfolio_id: str, strategy: str, action: str, position: int, order_reference: str, price: float, identity: str):
        stmt = Statement._build_insert_stmt(
            [TableOperation.PORTFOLIO_ID, TableOperation.STRATEGY, TableOperation.ACTION, TableOperation.POSITION,
             TableOperation.ORDER_REFERENCE, TableOperation.PRICE, TableOperation.IDENTITY],
            TableOperation.table_name,
            [portfolio_id, strategy, action, position, order_reference, price, identity])
        return stmt

    @staticmethod
    def build_stmt_position_by_entry_select_by_position(portfolio_id: str, strategy: str, market: str, symbol: str):
        '''
        Positions by entry ordered by creation date in descending order

        :param portfolio_id:
        :param strategy:
        :param market:
        :param symbol:
        :return:
        '''

        stmt = (f"select p.{TablePositionByEntry.POSITION},p.{TablePositionByEntry.AVG_PRICE},"
                f"p.{TablePositionByEntry.ORDER_REFERENCE},p.{TablePositionByEntry.STATE},p.{TablePositionByEntry.CREATED},"
                f"o.{TableOrder.ORDER_ID},o.{TableOrder.TYPE},o.{TableOrder.IS_BUY},o.{TableOrder.QUANTITY},"
                f"o.{TableOrder.PRICE},o.{TableOrder.ACTION},o.{TableOrder.REFERENCE},o.{TableOrder.COMMENT} from "
                f"{TablePositionByEntry.table_name} as p inner join {TableOrder.table_name} as o on "
                f"p.{TablePositionByEntry.SESSION_ID}=o.{TableOrder.SESSION_ID} and "
                f"p.{TablePositionByEntry.ORDER_ID}=o.{TableOrder.ORDER_ID} ")

        where_items = list()
        where_items.append((f"p.{TablePositionByEntry.PORTFOLIO_ID}", portfolio_id))
        where_items.append((f"p.{TablePositionByEntry.STRATEGY}", strategy))
        where_items.append((f"p.{TablePositionByEntry.MARKET}", market))
        where_items.append((f"p.{TablePositionByEntry.SYMBOL}", symbol))
        stmt += Statement._build_simple_where_clause(where_items)
        stmt += f" and p.{TablePositionByEntry.STATE} in ('{TablePositionByEntry.STATE_PENDING}','{TablePositionByEntry.STATE_FULLY_FILLED}')"
        stmt += f" order by p.{TablePositionByEntry.CREATED} desc"
        return stmt

    @staticmethod
    def build_stmt_position_by_entry_update(session_id: str = None, order_id: int = None, portfolio_id: str = None,
                                            strategy: str = None, order_reference: str = None, avg_price: float = None,
                                            state: str = None, position: int = None):
        cols = []
        values = []

        Statement._append_value(cols, values, TablePositionByEntry.AVG_PRICE, avg_price)
        Statement._append_value(cols, values, TablePositionByEntry.STATE, state)
        Statement._append_value(cols, values, TablePositionByEntry.POSITION, position)

        stmt = Statement._build_update_stmt(TablePositionByEntry.table_name, cols, values)

        where_items = list()
        if session_id is not None:
            where_items.append((TablePositionByEntry.SESSION_ID, session_id))
            where_items.append((TablePositionByEntry.ORDER_ID, order_id))
        elif order_reference is not None:
            where_items.append((TablePositionByEntry.PORTFOLIO_ID, portfolio_id))
            where_items.append((TablePositionByEntry.STRATEGY, strategy))
            where_items.append((TablePositionByEntry.ORDER_REFERENCE, order_reference))
        stmt += Statement._build_simple_where_clause(where_items)
        return stmt

    @staticmethod
    def build_stmt_session_insert(session_id: str, ip: str) -> str:
        return Statement._build_insert_stmt(['id', 'next_request_id', 'ip'], 'session', [session_id, '1', ip])

    @staticmethod
    def build_stmt_session_increment_next_request_id(session_id: str) -> str:
        return (f"update {TableSession.table_name} set {TableSession.NEXT_REQUEST_ID} = "
                f"{TableSession.NEXT_REQUEST_ID} + 1 where {TableSession.ID}='{session_id}'")

    @staticmethod
    def build_stmt_strategy_insert(strategy: str) -> str:
        stmt = Statement._build_insert_stmt([TableStrategy.ID, TableStrategy.DESCRIPTION], TableStrategy.table_name,
                                            [strategy, ''], ignore=True)
        return stmt

    @staticmethod
    def _append_condition(conditions: str, condition: str, relation: str = ' and '):
        return f'{relation if len(conditions) > 0 else ""}{condition}'

    @staticmethod
    def _append_value(cols, values, col_name, value):
        if value is not None:
            cols.append(col_name)
            values.append(value)
