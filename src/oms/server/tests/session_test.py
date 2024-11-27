# from collections import OrderedDict
#
# import pytest
# import ujson
#
# from oms.server.ledger.factory import LedgerFactory
# from oms.server.session import ClientSession
#
#
# class MockOms:
#     def __init__(self):
#         config = OrderedDict()
#
#         mysql_cfg = OrderedDict()
#         mysql_cfg['host'] = '127.0.0.1'
#         mysql_cfg['database'] = 'oms'
#         mysql_cfg['user'] = 'root'
#         mysql_cfg['password'] = 'Waverider1!'
#
#         db_cfg = OrderedDict()
#         db_cfg['mysql'] = mysql_cfg
#
#         config['ledger'] = db_cfg
#
#         self.ledger = LedgerFactory.create_ledger(config)
#
#
# @pytest.fixture
# def mock_oms():
#     return MockOms()
#
#
# @pytest.mark.skip(reason="avoid MySQL tests in CI")
# def test__build_position_message(mock_oms):
#     session = ClientSession('Client_Session_001', '0b01', mock_oms)
#     session._account_id = 'dev_account'
#     msg = session._build_position_message()
#     assert False, ujson.dumps(msg)
