from datetime import datetime, timezone

from oms.server.ledger.statement import Statement
from smartquant.execution.base import Action, OrderState


class TestStatement:
    def test__to_insert_value(self):
        v = Statement._to_insert_value(
            datetime(year=2011, month=10, day=20, hour=13, minute=20, second=34, tzinfo=timezone.utc))
        assert v == ''

    def test__build_insert_stmt(self):
        stmt = Statement._build_insert_stmt(['col1', 'col2', 'col3'], 'test_table', ['value1', 2, 'value3'])
        assert stmt == "insert into test_table (col1,col2,col3) values ('value1',2,'value3')"

        stmt = Statement._build_insert_stmt(['col1', 'col2', 'col3'], 'test_table', ['value1', 2, None])
        assert stmt == "insert into test_table (col1,col2,col3) values ('value1',2,null)"

        stmt = Statement._build_insert_stmt(['col1', 'col2', 'col3'], 'test_table',
                                            ['value1',
                                             datetime(year=2011, month=11, day=2, hour=23, minute=50, second=13),
                                             None])
        assert stmt == "insert into test_table (col1,col2,col3) values ('value1','2011-11-02 23:50:13',null)"

    def test__build_select_stmt(self):
        stmt = Statement._build_select_stmt(['col1', 'col2', 'col3'], 'test_table')
        assert stmt == 'select col1,col2,col3 from test_table where '

    def test__to_insert_value(self):
        assert Statement._to_insert_value(None) == 'null'
        assert Statement._to_insert_value(123) == '123'
        assert Statement._to_insert_value(123.45) == '123.45'
        assert Statement._to_insert_value('This is a string') == "'This is a string'"
        assert Statement._to_insert_value(
            datetime(year=2011, month=11, day=2, hour=23, minute=50, second=13)) == "'2011-11-02 23:50:13'"

    def test_build_stmt_account_select_by_id(self):
        stmt = Statement.build_stmt_account_select_by_id('simple_account')
        assert stmt == "select id,cash,currency from account where id='simple_account'"

    def test_build_stmt_execution_insert(self):
        stmt = Statement.build_stmt_execution_insert('a_broker', 'order_id_123', 'execution_456',
                                                     'gateway_order_id_123',
                                                     False, 'NQH1', 10, 123.45, 0, 20, 'USD',
                                                     datetime(year=2011, month=11, day=2, hour=23, minute=50,
                                                              second=13))
        assert stmt == ("insert into execution (broker_id,broker_order_id,broker_execution_id,gateway_order_id,is_buy,"
                        "contract,quantity,price,leave_quantity,commission,currency,execution_datetime) values ('a_broker','order_id_123',"
                        "'execution_456','gateway_order_id_123',False,'NQH1',10,123.45,0,20,'USD','2011-11-02 23:50:13')")

    def test_build_stmt_execution_select_by_broker_id_and_date(self):
        stmt = Statement.build_stmt_execution_select_by_broker_id_and_date('broker_123',
                                                                           'execution_123',
                                                                           datetime(year=2011, month=10, day=20,
                                                                                    hour=13,
                                                                                    minute=20, second=34))
        assert stmt == ("select broker_id,broker_order_id,broker_execution_id,gateway_order_id,is_buy,quantity,price,"
                        "leave_quantity,execution_datetime from execution where broker_id='broker_123' and "
                        "broker_execution_id='execution_123' and execution_datetime>='2011-10-20 13:20:34'")

        stmt = Statement.build_stmt_execution_select_by_broker_id_and_date('broker_123',
                                                                           'execution_123')
        assert stmt == ("select broker_id,broker_order_id,broker_execution_id,gateway_order_id,is_buy,quantity,price,"
                        "leave_quantity,execution_datetime from execution where broker_id='broker_123' and "
                        "broker_execution_id='execution_123'")

    def test_build_stmt_instrument_insert_or_update(self):
        stmt = Statement.build_stmt_instrument_insert_or_update('NYMEX', 'CL', 'CLX9',
                                                                datetime(year=2019, month=11, day=22))
        assert stmt == ("insert into instrument (market,symbol,code,expiry) values "
                        "('NYMEX','CL','CLX9','2019-11-22 00:00:00') on duplicate key update code='CLX9', "
                        "expiry='2019-11-22 00:00:00'")

    def test_build_stmt_instrument_select(self):
        stmt = Statement.build_stmt_instrument_select()
        assert stmt == "select market,symbol,code,expiry from instrument  "

    def test_build_stmt_order_insert(self):
        stmt = Statement.build_stmt_order_insert('client_session_000', 1234567890, 1234567890, 'IB_TWS',
                                                 'my_order_id_for_ib', 'CME', 'NQV9', 'limit', False, 10, 123.95, None,
                                                 'portfolio_1', 'entry', 'simple_strategy', 'client reference',
                                                 {'ATR': 0.96, 'pattern': 'dummy'})
        assert stmt == ("insert into order_ (session_id,order_id,parent_order_id,broker_id,broker_order_id,market,"
                        "symbol,type,is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment)"
                        " values ('client_session_000',1234567890,1234567890,'IB_TWS','my_order_id_for_ib','CME','NQV9',"
                        "'limit',False,10,123.95,'new',null,'portfolio_1','entry','simple_strategy','client reference',"
                        "'{\"ATR\":0.96,\"pattern\":\"dummy\"}')")

        stmt = Statement.build_stmt_order_insert('client_session_000', 1234567890, 1234567890, 'IB_TWS',
                                                 'my_order_id_for_ib', 'CME', 'NQV9', 'limit', False, 10, 123.95, None,
                                                 'portfolio_1', 'entry', 'simple_strategy', 'client reference', None)
        assert stmt == ("insert into order_ (session_id,order_id,parent_order_id,broker_id,broker_order_id,market,"
                        "symbol,type,is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment)"
                        " values ('client_session_000',1234567890,1234567890,'IB_TWS','my_order_id_for_ib','CME','NQV9',"
                        "'limit',False,10,123.95,'new',null,'portfolio_1','entry','simple_strategy','client reference',"
                        "null)")

    def test_build_stmt_order_select(self):
        stmt = Statement.build_stmt_order_select(session_id='session_id_123')
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123'")

        stmt = Statement.build_stmt_order_select(order_id='1234567')
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where order_id=1234567")

        stmt = Statement.build_stmt_order_select(broker_order_id='broker_order_id_123')
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from "
                        "order_ where broker_order_id='broker_order_id_123'")

        stmt = Statement.build_stmt_order_select(session_id='session_id_123', broker_order_id='broker_order_id_123')
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123' and broker_order_id='broker_order_id_123'")

        stmt = Statement.build_stmt_order_select(session_id='session_id_123', broker_order_id='broker_order_id_123',
                                                 active_orders_only=True)
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123' and broker_order_id='broker_order_id_123' and state in "
                        "('NEW','PENDING','ACTIVE','PARTICALLY_FILLED')")

        stmt = Statement.build_stmt_order_select(session_id='session_id_123', broker_order_id='broker_order_id_123',
                                                 symbol='CL', active_orders_only=True)
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123' and broker_order_id='broker_order_id_123' and symbol='CL' "
                        "and state in ('NEW','PENDING','ACTIVE','PARTICALLY_FILLED')")

        stmt = Statement.build_stmt_order_select(session_id='session_id_123', broker_order_id='broker_order_id_123',
                                                 symbol='CL', active_orders_only=True, order_by_last_modified=True)
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123' and broker_order_id='broker_order_id_123' and symbol='CL' "
                        "and state in ('NEW','PENDING','ACTIVE','PARTICALLY_FILLED') order by last_modified desc")

        stmt = Statement.build_stmt_order_select(session_id='session_id_123', broker_order_id='broker_order_id_123',
                                                 symbol='CL', action=Action.STOP_LOSS, active_orders_only=True,
                                                 order_by_last_modified=True)
        assert stmt == ("select session_id,order_id,parent_order_id,broker_id,broker_order_id,market,symbol,type,"
                        "is_buy,quantity,price,state,qualifier,portfolio,action,strategy,reference,comment,filled_quantity,remaining_quantity from order_ "
                        "where session_id='session_id_123' and broker_order_id='broker_order_id_123' and symbol='CL' "
                        "and action='STOP_LOSS' and state in ('NEW','PENDING','ACTIVE','PARTICALLY_FILLED') order by "
                        "last_modified desc")

    def test_build_stmt_order_update(self):
        stmt = Statement.build_stmt_order_update('ibtws_broker', '12345678', filled_quantity=1, remaining_quantity=9)
        assert stmt == ("update order_ set remaining_quantity=9,filled_quantity=1 where broker_id='ibtws_broker' and "
                        "broker_order_id='12345678'")

        stmt = Statement.build_stmt_order_update('ibtws_broker', '12345678', filled_quantity=3, remaining_quantity=7,
                                                 state=OrderState.ACTIVE)
        assert stmt == ("update order_ set remaining_quantity=7,filled_quantity=3,state='ACTIVE' where "
                        "broker_id='ibtws_broker' and broker_order_id='12345678'")

        stmt = Statement.build_stmt_order_update('ibtws_broker', '12345678', state=OrderState.CANCELLED)
        assert stmt == ("update order_ set state='CANCELLED' where broker_id='ibtws_broker' and "
                        "broker_order_id='12345678'")

        stmt = Statement.build_stmt_order_update('ibtws_broker', '12345678', quantity=123, price=10,
                                                 action=Action.ENTRY)
        assert stmt == ("update order_ set quantity=123,price=10,action='ENTRY' where broker_id='ibtws_broker' and "
                        "broker_order_id='12345678'")

    def test_build_stmt_portfolio_select_by_id_and_account_id(self):
        stmt = Statement.build_stmt_portfolio_select_by_id_and_account_id(portfolio_id='portfolio_1')
        assert stmt == ("select id,account_id from portfolio where id='portfolio_1'")

        stmt = Statement.build_stmt_portfolio_select_by_id_and_account_id(account_id='account_1')
        assert stmt == ("select id,account_id from portfolio where account_id='account_1'")

        stmt = Statement.build_stmt_portfolio_select_by_id_and_account_id(portfolio_id='portfolio_1',
                                                                          account_id='account_1')
        assert stmt == ("select id,account_id from portfolio where id='portfolio_1' and account_id='account_1'")

    def test_build_stmt_position_insert_or_update(self):
        stmt = Statement.build_stmt_position_insert_or_update('dev_portfolio', 'simple_strategy', 'CME', 'NQ', 7, 1.234)
        assert stmt == ("insert into position (portfolio_id,strategy,market,symbol,position,avg_price) values"
                        " ('dev_portfolio','simple_strategy','CME','NQ',7,1.234) on duplicate key update position=position+7, avg_price=1.234")

    def test_build_stmt_position_select(self):
        stmt = Statement.build_stmt_position_select(portfolio_id='dev_portfolio')
        assert stmt == ("select portfolio_id,strategy,market,symbol,position,avg_price from position where "
                        "portfolio_id='dev_portfolio'")

        stmt = Statement.build_stmt_position_select(strategy='simple_strategy')
        assert stmt == ("select portfolio_id,strategy,market,symbol,position,avg_price from position where "
                        "strategy='simple_strategy'")

        stmt = Statement.build_stmt_position_select(portfolio_id='dev_portfolio', strategy='simple_strategy')
        assert stmt == ("select portfolio_id,strategy,market,symbol,position,avg_price from position where "
                        "portfolio_id='dev_portfolio' and strategy='simple_strategy'")

        stmt = Statement.build_stmt_position_select(portfolio_id='dev_portfolio', strategy='simple_strategy',
                                                    symbol='CL')
        assert stmt == ("select portfolio_id,strategy,market,symbol,position,avg_price from position where "
                        "portfolio_id='dev_portfolio' and strategy='simple_strategy' and symbol='CL'")

        stmt = Statement.build_stmt_position_select(symbol='CL')
        assert stmt == ("select portfolio_id,strategy,market,symbol,position,avg_price from position where "
                        "symbol='CL'")

    def test_build_stmt_position_sum(self):
        stmt = Statement.build_stmt_position_sum('CL')
        assert stmt == ("select symbol,sum(position) as position from position where symbol='CL'")

    def test_build_stmt_strategy_insert(self):
        stmt = Statement.build_stmt_strategy_insert('test_strategy')
        assert stmt == ("insert ignore into strategy (id,description) values ('test_strategy','')")

    def test_build_stmt_find_account_portfolio_strategy(self):
        stmt = Statement.build_stmt_find_account_portfolio_strategy('WRCA001', 'WRCAP001', 'sample_strategy')
        assert stmt == ("select a.id, p.id, s.id from account as a inner join portfolio as p inner join strategy as s "
                        "on a.id=p.account_id where a.id='WRCA001' and p.id='WRCAP001' and s.id='sample_strategy'")

    def test_build_stmt_position_by_entry_insert(self):
        stmt = Statement.build_stmt_position_by_entry_insert('portfolio_101', 'sample_strategy', 'GLOBEX', 'NQ', 10,
                                                             0.0, 'client_session_000', 1234567, 'PENDING',
                                                             'order_ref_123')
        assert stmt == ("insert into position_by_entry (portfolio_id,strategy,market,symbol,position,avg_"
                        "price,session_id,order_id,state,order_reference) values ('portfolio_101','sample_strategy',"
                        "'GLOBEX','NQ',10,0.0,'client_session_000',1234567,'PENDING','order_ref_123')")

    def test_build_stmt_position_by_entry_update(self):
        stmt = Statement.build_stmt_position_by_entry_update('client_session_000', 1234567, avg_price=2.345)
        assert stmt == ("update position_by_entry set avg_price=2.345 where session_id='client_session_000' and "
                        "order_id=1234567")

        stmt = Statement.build_stmt_position_by_entry_update('client_session_000', 1234567, state='FULLY_FILLED')
        assert stmt == ("update position_by_entry set state='FULLY_FILLED' where session_id='client_session_000' and "
                        "order_id=1234567")

        stmt = Statement.build_stmt_position_by_entry_update('client_session_000', 1234567, avg_price=2.345,
                                                             state='FULLY_FILLED')
        assert stmt == ("update position_by_entry set avg_price=2.345,state='FULLY_FILLED' where "
                        "session_id='client_session_000' and order_id=1234567")

        stmt = Statement.build_stmt_position_by_entry_update(portfolio_id='portfolio_101', strategy='simple_strategy',
                                                             order_reference='order_reference_123', avg_price=2.345,
                                                             state='FULLY_FILLED')
        assert stmt == ("update position_by_entry set avg_price=2.345,state='FULLY_FILLED' where "
                        "portfolio_id='portfolio_101' and strategy='simple_strategy' and "
                        "order_reference='order_reference_123'")

    def test_build_stmt_position_by_entry_select_by_position(self):
        stmt = Statement.build_stmt_position_by_entry_select_by_position('portfolio_101', 'simple_strategy', 'GLOBEX',
                                                                         'NQ')
        assert stmt == ("select p.position,p.avg_price,p.order_reference,p.state,p.created,o.order_id,o.type,o.is_buy,"
                        "o.quantity,o.price,o.action,o.reference,o.comment from position_by_entry as p inner join "
                        "order_ as o on p.session_id=o.session_id and p.order_id=o.order_id where "
                        "p.portfolio_id='portfolio_101' and p.strategy='simple_strategy' and p.market='GLOBEX' and "
                        "p.symbol='NQ' and p.state in ('PENDING','FULLY_FILLED') order by p.created desc")
