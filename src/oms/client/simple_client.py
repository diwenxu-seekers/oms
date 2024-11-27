import argparse
import asyncio
import logging
import logging.handlers
import sys
from datetime import datetime
from logging import debug, info

import ujson

from oms.client.client import OmsClient
from oms.common.message import OmsMessageError, OmsMessageExecution, OmsMessagePosition
from oms.server.ledger.statement import TableOrder
from smartquant.common.market import Market
from smartquant.common.utils import create_loop, start_loop
from smartquant.execution.base import Action, OrderType

LOGGING_FORMAT = '%(asctime)s;%(levelname)s;%(name)s;%(process)d;%(threadName)s;%(funcName)s;%(message)s'
STRATEGY_NAME = 'simple_strategy12345678'


async def wait_for_input(client: OmsClient, loop: asyncio.AbstractEventLoop):
    info('Wait for OMS client to be ready...')
    await client.wait_till_ready()
    info('OMS client is ready')

    last_order_ref = None
    while loop.is_running():
        sys.stdout.write('Command: ')
        sys.stdout.flush()
        line = await loop.run_in_executor(None, sys.stdin.readline)
        line = line.strip()
        quantity = 1

        if line.lower() == 'entry-buy':
            last_order_ref = str(datetime.now())
            client.place_order(market=Market.GLOBEX, symbol='NQ', order_type=OrderType.MKT, is_buy=True,
                               quantity=quantity, price=0, portfolio='WRCP001', action=Action.ENTRY,
                               strategy=STRATEGY_NAME, reference='test_client',
                               comment={'ATR': 0.83, 'pattern': 'bnr', 'stop_loss_offset': -10,
                                        'stop_loss_absolute': 7299,
                                        TableOrder.COMMENT_ORDER_REFERENCE: last_order_ref})
        elif line.lower() == 'entry-sell':
            comment = {'ATR': 0.83, 'pattern': 'bnr', 'stop_loss_offset': 10}
            client.place_order(market=Market.GLOBEX, symbol='NQ', order_type=OrderType.MKT, is_buy=False,
                               quantity=quantity, price=0, portfolio='WRCP001', action=Action.ENTRY,
                               strategy=STRATEGY_NAME, reference='test_client', comment=comment)
        elif line.lower() == 'exit-buy':
            client.place_order(market=Market.GLOBEX, symbol='NQ', order_type=OrderType.MKT, is_buy=True,
                               quantity=quantity, price=0, portfolio='WRCP001', action=Action.EXIT,
                               strategy=STRATEGY_NAME, reference='test_client',
                               comment={'ATR': 0.83, 'pattern': 'bnr'})
        elif line.lower() == 'exit-sell':
            comment = {'ATR': 0.83, 'pattern': 'bnr'}
            if last_order_ref is not None:
                comment[TableOrder.COMMENT_ORDER_REFERENCE] = last_order_ref
            client.place_order(market=Market.GLOBEX, symbol='NQ', order_type=OrderType.MKT, is_buy=False,
                               quantity=quantity, price=0, portfolio='WRCP001', action=Action.EXIT,
                               strategy=STRATEGY_NAME, reference='test_client', comment=comment)
            last_order_ref = None
        elif line.lower() == 'req-pos':
            client.request_position()
        elif line.lower() == 'quit':
            loop.stop()
            logging.info(f'Exiting...')
            return True
        else:
            logging.info(f'Unknown command: [{line}]')

    return True


def configure_logging(args):
    level = getattr(logging, args.log_level) if args.log_level else logging.INFO
    logging.basicConfig(format=LOGGING_FORMAT, level=level)


def configure_parser():
    parser = argparse.ArgumentParser(prog=__package__)
    parser.add_argument('--log-level', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], help='Log level')
    parser.add_argument('--conn', type=str, help='Connection string')
    return parser


def preprocessing():
    parser = configure_parser()
    args = parser.parse_args()
    configure_logging(args)
    debug(args)
    return args


def connection_state_callback(flag: bool, msg: str):
    info(f'{flag},{msg}')


def error_callback(msg: OmsMessageError):
    info(ujson.dumps(msg, indent=2))


def execution_callback(msg: OmsMessageExecution):
    info(ujson.dumps(msg, indent=2))


def position_callback(msg: OmsMessagePosition):
    info(ujson.dumps(msg, indent=2))


def main():
    args = preprocessing()
    client = OmsClient(args.conn, 'test_client', 'WRCA001', {STRATEGY_NAME: 'WRCP001'})
    client.set_connection_state_callback(connection_state_callback)
    client.set_error_callback(error_callback)
    client.set_execution_callback(execution_callback)
    client.set_position_callback(position_callback)

    with create_loop() as loop:
        client.install_loop(loop)
        asyncio.ensure_future(wait_for_input(client, loop))
        start_loop(loop)

    return 0


if __name__ == "__main__":
    start_time = datetime.now()
    try:
        exit(main())
    finally:
        info(f'Finished. Total time elapsed: {datetime.now() - start_time}')
