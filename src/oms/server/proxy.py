import asyncio
import logging
from asyncio import AbstractEventLoop
from collections import OrderedDict

import zmq
from zmq.asyncio import Context, Poller

from oms.common.config import CFG_BACKEND, CFG_FRONTEND, CFG_MESSAGING, CFG_PROXY


class LocalBroker:
    def __init__(self, config: OrderedDict):
        self._logger = logging.getLogger(__name__)
        self._config = config
        self._context = Context()

    def install_loops(self, loop: AbstractEventLoop):
        if CFG_PROXY in self._config[CFG_MESSAGING]:
            asyncio.ensure_future(self.run(loop))

    async def run(self, loop: AbstractEventLoop):
        self._logger.info(f'Start local broker...')

        frontend = self._context.socket(zmq.ROUTER)
        backend = self._context.socket(zmq.DEALER)

        cfg = self._config[CFG_MESSAGING][CFG_PROXY]
        frontend.bind(cfg[CFG_FRONTEND])
        backend.bind(cfg[CFG_BACKEND])
        self._logger.info(f'Frontend listening at {cfg[CFG_FRONTEND]}, backend listening at {cfg[CFG_BACKEND]}')

        poller = Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(backend, zmq.POLLIN)

        while loop.is_running():
            socks = dict(await poller.poll())
            if socks.get(frontend) == zmq.POLLIN:
                msg = await frontend.recv_multipart()
                self._logger.debug(f'Frontend receives: {msg}')
                backend.send_multipart(msg)

            if socks.get(backend) == zmq.POLLIN:
                msg = await backend.recv_multipart()
                self._logger.debug(f'Backend receives: {msg}')
                frontend.send_multipart(msg)
