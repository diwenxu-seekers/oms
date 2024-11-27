import asyncio
import logging
import time
from asyncio import AbstractEventLoop
from datetime import datetime, timedelta
from threading import Lock
from typing import Callable, Dict

import zmq
from zmq.asyncio import Context, Poller

from oms.common.message import (ErrorCode, Heartbeat, MsgType, OmsMessage, OmsMessageError, OmsMessageExecution,
                                OmsMessageHeartbeat, OmsMessageInit, OmsMessageNewOrder, OmsMessagePosition)
from smartquant.common.market import Market
from smartquant.execution.base import Action, OrderType


class OmsClient:
    def __init__(self, uri: str, session_name: str, account: str, strategies: Dict[str, str]):
        self._logger = logging.getLogger(__name__)
        self._uri = uri
        self._session = session_name
        self._account = account
        self._strategies = strategies
        self._context = Context()
        self._socket = None
        self._is_connected = False
        self._is_connection_ready = False
        self._request_id = None
        self._lock = Lock()
        self._callback_connection_state: Callable[[bool, str], None] = None
        self._callback_error: Callable[[OmsMessageError], None] = None
        self._callback_execution: Callable[[OmsMessageExecution], None] = None
        self._callback_position: Callable[[OmsMessagePosition], None] = None

    @property
    def is_connected(self):
        return self._is_connected

    def set_connection_state_callback(self, callback: Callable[[bool, str], None]):
        self._callback_connection_state = callback

    def set_error_callback(self, callback: Callable[[OmsMessageError], None]):
        self._callback_error = callback

    def set_execution_callback(self, callback: Callable[[OmsMessageExecution], None]):
        self._callback_execution = callback

    def set_position_callback(self, callback: Callable[[OmsMessagePosition], None]):
        self._callback_position = callback

    def install_loop(self, loop: asyncio.AbstractEventLoop):
        asyncio.ensure_future(self.run(loop))
        asyncio.ensure_future(self.run_heartbeat(loop))

    async def run(self, loop: asyncio.AbstractEventLoop):
        retry_interval = Heartbeat.RETRY_INTERVAL
        msg_interval = timedelta(seconds=30)
        last_msg_time = datetime.max

        self._logger.info(f'Start to connect to {self._uri}...')
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.SNDHWM, 1)
        self._socket.connect(self._uri)
        poller = Poller()
        poller.register(self._socket, zmq.POLLIN)

        while loop.is_running():
            self.send_init()
            last_server_heartbeat = datetime.now()

            while loop.is_running():
                socks = dict(await poller.poll(timeout=1))

                if socks.get(self._socket) == zmq.POLLIN:
                    msg = await self._socket.recv()

                    decoded = OmsMessage.from_json(msg)

                    lvl = logging.DEBUG if decoded.msg_type == MsgType.HEARTBEAT else logging.INFO
                    self._logger.log(lvl, f'Received message: {decoded}')

                    if decoded.msg_type != MsgType.ERROR:
                        last_server_heartbeat = datetime.now()

                    if decoded.msg_type == MsgType.DELETE_ORDER:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.ERROR:
                        if decoded.error_code == ErrorCode.ALREADY_LOGGED_IN:
                            self._logger.info(f'Already logged in, the INIT message is not necessary')
                            continue
                        elif decoded.error_code in [ErrorCode.DUPLICATED_SESSION_ID, ErrorCode.NOT_LOGGED_IN,
                                                    ErrorCode.INIT_ERROR]:
                            self._logger.warning(f'Login rejected, will retry in {retry_interval} seconds...')
                            retry_interval = await self._wait_to_retry(retry_interval)
                            break
                        if self._callback_error is not None:
                            self._callback_error(decoded)
                    elif decoded.msg_type == MsgType.EXECUTION:
                        if self._callback_execution is not None:
                            self._callback_execution(decoded)
                    elif decoded.msg_type == MsgType.EXECUTION_HISTORY:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.HEARTBEAT:
                        if self._is_connection_ready != decoded.is_ready:
                            self._is_connection_ready = decoded.is_ready
                            msg = 'OMS is ready' if self.is_ready else 'OMS is not ready'
                            self._call_connection_state_callback(msg)
                    elif decoded.msg_type == MsgType.INIT:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.MODIFY_ORDER:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.NEW_ORDER:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.NEXT_REQUEST_ID:
                        self._request_id = decoded.next_request_id
                        self._is_connected = True
                        retry_interval = Heartbeat.RETRY_INTERVAL
                        self._call_connection_state_callback('Connected to OMS')
                    elif decoded.msg_type == MsgType.ORDER_STATUS:
                        raise NotImplementedError(f'{decoded.msg_type}')
                    elif decoded.msg_type == MsgType.POSITION:
                        if self._callback_position is not None:
                            self._callback_position(decoded)

                if Heartbeat.is_expired(last_server_heartbeat):
                    if self._is_connected:
                        self._is_connected = False
                        self._logger.warning(f'Lost heartbeat from OMS server, try to reconnect...')
                        self._call_connection_state_callback('Lost connection to OMS')
                        last_msg_time = datetime.now()
                        break
                    elif last_msg_time < datetime.now() - msg_interval:
                        last_msg_time = datetime.now()
                        self._logger.warning(f'No response from OMS yet, try to reconnect...')
                        break

        self._socket.close()
        self._logger.info('Listener thread stops')
        return True

    async def run_heartbeat(self, loop: asyncio.AbstractEventLoop):
        while loop.is_running():
            if self._is_connected:
                out_msg = OmsMessageHeartbeat()
                out_msg.timestamp = datetime.now().isoformat()
                out_msg.next = datetime.fromtimestamp(time.time() + Heartbeat.INTERVAL).isoformat()
                self._send(out_msg)
            await asyncio.sleep(Heartbeat.INTERVAL)

    def send_init(self):
        message = OmsMessageInit()
        message.account_id = self._account
        message.session_id = self._session
        message.strategies = self._strategies
        self._send(message)

    def place_order(self, market: Market, symbol: str, order_type: OrderType, is_buy: bool, quantity: int, price: float,
                    portfolio: str, action: Action, strategy: str, reference: str, comment: Dict[str, str]):
        msg = OmsMessageNewOrder()
        msg.request_id = self._next_request_id()
        msg.market = market.value
        msg.symbol = symbol
        msg.order_type = order_type.value
        msg.is_buy = is_buy
        msg.quantity = quantity
        msg.price = price
        msg.portfolio = portfolio
        msg.action = action.value
        msg.strategy = strategy
        msg.reference = reference
        msg.comment = comment
        self._send(msg)
        return msg.request_id

    def request_position(self):
        msg = OmsMessagePosition()
        msg.request_id = self._next_request_id()
        self._send(msg)

    async def wait_till_ready(self, loop: AbstractEventLoop = None):
        while not self._is_connected and (loop is None or loop.is_running()):
            await asyncio.sleep(0.1)

    @property
    def is_ready(self):
        return self._is_connected and self._is_connection_ready

    @staticmethod
    async def _wait_to_retry(interval: int):
        await asyncio.sleep(interval)
        return min(interval * 2, Heartbeat.MAX_RETRY_INTERVAL)

    def _call_connection_state_callback(self, *args):
        if self._callback_connection_state is not None:
            self._callback_connection_state(self.is_ready, *args)

    def _next_request_id(self):
        with self._lock:
            r = self._request_id
            self._request_id += 1
        return r

    def _send(self, msg: OmsMessage):
        self._logger.debug(f'Send message: {msg}')
        self._socket.send(msg.to_bytes())
