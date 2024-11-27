# Sample setup SQL scripts for UAT

# Setup account
INSERT INTO oms.account (id, cash, currency) VALUES ('WRCA001', 10000000.00000, 'USD');

# Setup portfolio
INSERT INTO oms.portfolio (id, account_id) VALUES ('WRCP001', 'WRCA001');

# Setup strategy details for auto contract roll
INSERT INTO oms.strategy (id, description) VALUES ('OMS', '');

# Setup markets
INSERT INTO oms.market (market) VALUES ('CME');
INSERT INTO oms.market (market) VALUES ('GLOBEX');
INSERT INTO oms.market (market) VALUES ('NYMEX');