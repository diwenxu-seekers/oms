from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List

import ujson
from oms.server.ledger.statement import TableOperation
from smartquant.common.message import JsonMessage


class Heartbeat:
    """
    This is a contract shared between server and client.
    i.e. all change must be deployed to both server and client side.
    """
    INTERVAL = 15  # heartbeat sent every n sec
    LIVENESS = 5  # At most can miss 5 heartbeats
    MAX_RETRY_INTERVAL = 32  # AT most wait for 32 seconds to retry
    RETRY_INTERVAL = 2  # Wait for 2 seconds before first retry

    @staticmethod
    def is_expired(last: datetime):
        if last is not None:
            return datetime.now() > last + Heartbeat.LIVENESS * timedelta(seconds=Heartbeat.INTERVAL)
        return False


class ErrorCode:
    SYSTEM_ERROR = 100
    DUPLICATED_SESSION_ID = 101
    BAD_REQUEST_ID = 102
    ALREADY_LOGGED_IN = 103
    NOT_LOGGED_IN = 104
    INIT_ERROR = 105
    ORDER_ERROR = 106
    ORDER_REJECTED = 107


class Msg:
    ACCOUNT = 'account'
    GROUP = 'group'
    ITEMS = 'items'
    MSG_TYPE = 'msg_type'
    OMS = 'oms'
    ORDER = 'order'
    ORDERS = 'orders'
    PORTFOLIOS = 'portfolios'
    POSITIONS = 'positions'
    POSITIONS_BY_ENTRY = 'positions_by_entry'


class MsgType:
    DELETE_ORDER = 'delete_order'
    ERROR = 'error'
    EXECUTION = 'execution'
    EXECUTION_HISTORY = 'execution_history'
    HEARTBEAT = 'heartbeat'
    INIT = 'init'
    MODIFY_ORDER = 'modify_order'
    NEW_ORDER = 'new_order'
    NEXT_REQUEST_ID = 'next_request_id'
    ORDER_STATUS = 'order_status'
    POSITION = 'position'


ENCODING = 'utf-8'


class OmsMessage(JsonMessage):
    @staticmethod
    def from_json(json_str: str):
        msg = ujson.loads(json_str)
        if msg[Msg.GROUP] != Msg.OMS:
            raise ValueError(f'Expect message group {Msg.OMS}, get {msg[Msg.GROUP]}')
        msg_type = msg[Msg.MSG_TYPE]

        if msg_type == MsgType.INIT:
            return OmsMessageInit(msg)
        elif msg_type == MsgType.NEXT_REQUEST_ID:
            return OmsMessageNextRequestId(msg)
        elif msg_type == MsgType.NEW_ORDER:
            return OmsMessageNewOrder(msg)
        # elif msg_type == MsgType.ORDER_STATUS:
        #     return OmsMessageOrderStatus(msg)
        elif msg_type == MsgType.EXECUTION:
            return OmsMessageExecution(msg)
        elif msg_type == MsgType.POSITION:
            return OmsMessagePosition(msg)
        elif msg_type == MsgType.HEARTBEAT:
            return OmsMessageHeartbeat(msg)
        elif msg_type == MsgType.ERROR:
            return OmsMessageError(msg)

        raise ValueError(f'Unsupported message type: {msg_type}')

    def __init__(self, msg_type: str):
        super().__init__(Msg.OMS, msg_type)

    def __str__(self):
        return str(self.__dict__)


class OmsMessageError(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.ERROR)
        self.error_code: int = None
        self.message: str = None
        self.session_id: str = None
        self.request_id: int = None
        self.read_msg(msg)


class OmsMessageInit(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.INIT)
        self.session_id: str = None
        self.account_id: str = None
        self.strategies: Dict[str, str] = None
        self.read_msg(msg)


class OmsMessageNextRequestId(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.NEXT_REQUEST_ID)
        self.next_request_id: int = None
        self.read_msg(msg)


class OmsMessageExecutionHistory(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.EXECUTION_HISTORY)
        self.read_msg(msg)
        # TODO: not required atm


class OmsMessageOrderStatus(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.ORDER_STATUS)
        self.read_msg(msg)
        # TODO: not required atm


class OmsMessageNewOrder(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.NEW_ORDER)
        self.request_id: int = None
        self.market: str = None
        self.symbol: str = None
        self.order_type: str = None
        self.is_buy: bool = None
        self.quantity: int = None
        self.price: Decimal = None
        self.portfolio: str = None
        self.action: str = None
        self.strategy: str = None
        self.reference: str = None
        self.comment: Dict[str, str] = None
        self.read_msg(msg)


class OmsMessageModifyOrder(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.MODIFY_ORDER)
        self.read_msg(msg)
        # TODO: not required atm


class OmsMessageDeleteOrder(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.DELETE_ORDER)
        self.request_id = None
        self.order_id = None
        self.read_msg(msg)
        # TODO: not required atm


class OmsMessageExecution(OmsMessage):
    class ItemExecution(JsonMessage):
        def __init__(self, msg: dict = None):
            self.order_id: str = None
            self.execution_id: str = None
            self.execution_time: str = None
            self.market: str = None
            self.symbol: str = None
            self.is_buy: bool = None
            self.quantity: int = None
            self.price: float = None
            self.remaining_quantity: int = None
            self.portfolio: str = None
            self.strategy: str = None
            self.action: str = None
            self.reference: str = None
            self.comment: Dict[str, str] = {}
            self.read_msg(msg)

    def __init__(self, msg: dict = None):
        super().__init__(MsgType.EXECUTION)
        self.request_id: int = None
        self.items: List[OmsMessageExecution.ItemExecution] = []
        self.read_msg(msg)
        if msg is not None:
            self.items = []
            for item in msg[Msg.ITEMS]:
                self.items.append(self.ItemExecution(item))


class OmsMessagePosition(OmsMessage):
    class ItemOrder(JsonMessage):
        def __init__(self, msg: dict = None):
            self.order_id: int = None
            self.market = None
            self.symbol = None
            self.order_type = None
            self.is_buy: bool = None
            self.quantity: int = None
            self.price: Decimal = None
            self.portfolio: str = None
            self.action: str = None
            self.strategy: str = None
            self.reference: str = None
            self.comment: Dict[str, str] = None
            self.read_msg(msg)

    class ItemPositionByEntry(JsonMessage):
        def __init__(self, msg: dict = None):
            self.position: int = None
            self.avg_price: float = None
            self.state = None
            self.created = None
            self.operations = None
            self.read_msg(msg)
            if msg is not None:
                try:
                    self.order = OmsMessagePosition.ItemOrder(msg[Msg.ORDER])
                except KeyError:
                    self.order = None
            if self.operations:
                for item in self.operations:
                    if TableOperation.CREATED in item:
                        item[TableOperation.CREATED] = datetime.fromtimestamp(item[TableOperation.CREATED])

    class ItemPosition(JsonMessage):
        def __init__(self, msg: dict = None):
            self.strategy: str = None
            self.market: str = None
            self.symbol: str = None
            self.position: int = None
            self.avg_price: float = None
            self.force_renew: bool = False
            self.read_msg(msg)
            if msg is not None:
                self.positions_by_entry = []
                try:
                    for item in msg[Msg.POSITIONS_BY_ENTRY]:
                        self.positions_by_entry.append(OmsMessagePosition.ItemPositionByEntry(item))
                except KeyError:
                    pass

    class ItemPortfolio(JsonMessage):
        def __init__(self, msg: dict = None):
            self.id: str = None
            self.positions: List[OmsMessagePosition.ItemPosition] = []
            self.read_msg(msg)
            if msg is not None:
                self.positions = []
                for item in msg[Msg.POSITIONS]:
                    self.positions.append(OmsMessagePosition.ItemPosition(item))

    class ItemAccount(JsonMessage):
        def __init__(self, msg: dict = None):
            self.id: str = None
            self.cash: float = None
            self.currency: str = None
            self.portfolios: List[OmsMessagePosition.ItemPortfolio] = []
            self.read_msg(msg)
            if msg is not None:
                self.portfolios: List[OmsMessagePosition.ItemPortfolio] = []
                for item in msg[Msg.PORTFOLIOS]:
                    self.portfolios.append(OmsMessagePosition.ItemPortfolio(item))

    def __init__(self, msg: dict = None):
        super().__init__(MsgType.POSITION)
        self.request_id: int = None
        self.account: OmsMessagePosition.ItemAccount = None
        self.read_msg(msg)
        if msg is not None:
            self.account = self.ItemAccount(msg[Msg.ACCOUNT])


class OmsMessageHeartbeat(OmsMessage):
    def __init__(self, msg: dict = None):
        super().__init__(MsgType.HEARTBEAT)
        self.timestamp: str = None
        self.next: str = None
        self.is_ready: bool = None
        self.message: str = None
        self.read_msg(msg)
