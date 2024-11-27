import logging
from collections import OrderedDict
from datetime import datetime, timedelta
from threading import RLock

import gateway_lib as gl
from oms.common.config import (CFG_CLIENT_ID, CFG_HOST, CFG_INTERACTIVE_BROKER, CFG_JOURNAL_FILE, CFG_NAME, CFG_PORT,
                               CFG_RECONNECT_INTERVAL_IN_SEC, CFG_TYPE)


class Broker:
    def __init__(self, config: OrderedDict, gateway: gl.AbstractGateway):
        self._logger = logging.getLogger(__name__)

        self._last_connection_try = datetime.now()
        self._gateway = gateway
        self._reconnect_interval_in_sec = config[CFG_RECONNECT_INTERVAL_IN_SEC]
        self._is_connected = False
        self._is_connecting = False
        self._lock = RLock()

    def connect(self):
        self._is_connecting = True
        with self._lock:
            try:
                self._gateway.connect()
            finally:
                self._is_connecting = False

    @property
    def gateway(self):
        return self._gateway

    @property
    def is_connected(self):
        return self._is_connected

    @is_connected.setter
    def is_connected(self, val: bool):
        self._logger.info(f'Broker {self._gateway.name}, set connected to {val}')
        changed = self._is_connected != val
        self._is_connected = val

        # Perform recovery procedures on toggled to connected=True.
        if val and changed:
            self._gateway.request_executions()
            self._gateway.request_open_orders()

    @property
    def is_connecting(self):
        return self._is_connecting

    @property
    def is_healthy(self):
        return self.gateway.is_healthy

    @property
    def name(self):
        return self.gateway.name

    @property
    def reconnect_interval_in_sec(self):
        return self._reconnect_interval_in_sec

    def cancel_order(self, *args, **kwargs):
        try:
            self.gateway.cancel_order(*args, **kwargs)
        except BrokenPipeError as e:
            self._handle_broken_pipe(e)

    def disconnect(self):
        self.gateway.disconnect()

    def is_time_to_reconnect(self):
        if self._reconnect_interval_in_sec <= 0:
            return False

        now = datetime.now()
        if self._last_connection_try + timedelta(seconds=self._reconnect_interval_in_sec) < now:
            self._last_connection_try = now
            return True

    def modify_order(self, *args, **kwargs):
        with self._lock:
            try:
                self.gateway.modify_order(*args, **kwargs)
            except BrokenPipeError as e:
                self._handle_broken_pipe(e)

    def ping(self):
        with self._lock:
            try:
                self._gateway.ping()
            except BrokenPipeError as e:
                self._handle_broken_pipe(e)

    def place_order(self, *args, **kwargs):
        with self._lock:
            try:
                self.gateway.place_order(*args, **kwargs)
            except BrokenPipeError as e:
                self._handle_broken_pipe(e)

    def _handle_broken_pipe(self, e: BrokenPipeError):
        self.is_connected = False
        self._gateway.disconnect()
        self._logger.exception(e)


class BrokerFactory:
    _logger = logging.getLogger(__name__)

    @staticmethod
    def create_broker(config: OrderedDict) -> Broker:
        if config[CFG_TYPE] == CFG_INTERACTIVE_BROKER:
            BrokerFactory._logger.info('Initialize IB gateway...')

            gw = gl.IBGateway(name=config[CFG_NAME], host=config[CFG_HOST], port=config[CFG_PORT],
                              client_id=config[CFG_CLIENT_ID], state_filepath=config[CFG_JOURNAL_FILE])
            gw.load_state()

            return Broker(config, gw)

        raise ValueError('Can\'t find any gateway configuration')
