import argparse
import logging
import logging.handlers
from datetime import datetime
from logging import debug, info

from oms.server.oms import Oms
from oms.server.proxy import LocalBroker
from smartquant.common.instrument import InstrumentRepository
from smartquant.common.utils import create_loop, setup_logging, start_loop, yamls2dict

HELP_MSG_DATETIME_FORMAT = 'YYYY-mm-ddTHH:MM:SS'
LOGGING_FORMAT = '%(asctime)s;%(levelname)s;%(name)s;%(process)d;%(threadName)s;%(funcName)s;%(message)s'


def configure_logging(args):
    level = getattr(logging, args.log_level) if args.log_level else logging.INFO
    logging.basicConfig(format=LOGGING_FORMAT, level=level)


def configure_parser():
    parser = argparse.ArgumentParser(prog=__package__)
    parser.add_argument('--log-level', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], help='Log level')
    parser.add_argument('-c', '--cfg', metavar='cfg', action='store', nargs='*', default=['oms.yml'],
                        help='OMS configuration file(s). If there are multiple configuration files, their contents '
                             'are merged, when there is a conflict then the file that appears later in the list '
                             'overrides those come in front.')
    return parser


def preprocessing():
    parser = configure_parser()
    args = parser.parse_args()
    configure_logging(args)
    info('OMS bootstrap starts')
    debug(args)
    return args


def main():
    args = preprocessing()
    config = yamls2dict(args.cfg)
    setup_logging(args.log_level, config)
    broker = LocalBroker(config)
    oms = Oms(config)

    InstrumentRepository(config)

    with create_loop() as loop:
        broker.install_loops(loop)
        oms.install_loops(loop)
        oms.init(loop)
        start_loop(loop)

    oms.close()
    return 0


if __name__ == "__main__":
    start_time = datetime.now()
    try:
        exit(main())
    finally:
        info(f'Finished. Total time elapsed: {datetime.now() - start_time}')
