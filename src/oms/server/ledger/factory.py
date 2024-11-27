from collections import OrderedDict

from oms.common.config import CFG_LEDGER, CFG_MYSQL
from .db import DbMySql


class LedgerFactory:
    @staticmethod
    def create_ledger(config: OrderedDict):
        cfg = config[CFG_LEDGER]

        if CFG_MYSQL in cfg:
            return DbMySql(cfg)

        raise ValueError('Can\'t find any ledger configuration')
