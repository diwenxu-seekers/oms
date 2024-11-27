-- MySQL dump 10.13  Distrib 5.7.30-33, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: oms
-- ------------------------------------------------------
-- Server version       5.7.30-33

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
/*!50717 SELECT COUNT(*) INTO @rocksdb_has_p_s_session_variables FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'performance_schema' AND TABLE_NAME = 'session_variables' */;
/*!50717 SET @rocksdb_get_is_supported = IF (@rocksdb_has_p_s_session_variables, 'SELECT COUNT(*) INTO @rocksdb_is_supported FROM performance_schema.session_variables WHERE VARIABLE_NAME=\'rocksdb_bulk_load\'', 'SELECT 0') */;
/*!50717 PREPARE s FROM @rocksdb_get_is_supported */;
/*!50717 EXECUTE s */;
/*!50717 DEALLOCATE PREPARE s */;
/*!50717 SET @rocksdb_enable_bulk_load = IF (@rocksdb_is_supported, 'SET SESSION rocksdb_bulk_load = 1', 'SET @rocksdb_dummy_bulk_load = 0') */;
/*!50717 PREPARE s FROM @rocksdb_enable_bulk_load */;
/*!50717 EXECUTE s */;
/*!50717 DEALLOCATE PREPARE s */;

--
-- Current Database: `oms`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `oms` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `oms`;

--
-- Table structure for table `account`
--

DROP TABLE IF EXISTS `account`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `account` (
  `id` varchar(100) NOT NULL,
  `cash` decimal(20,5) DEFAULT NULL,
  `currency` varchar(3) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `account_id_uindex` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_insert_account
after INSERT on account
for each row
begin
    insert into account_log (table_action, id, cash, currency) values ('insert', NEW.id, NEW.cash, NEW.currency);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_update_account
after UPDATE on account
for each row
begin
    insert into account_log (table_action, id, cash, currency) values ('update', NEW.id, NEW.cash, NEW.currency);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_delete_account
after DELETE on account
for each row
begin
    insert into account_log (table_action, id, cash, currency) values ('delete', OLD.id, OLD.cash, OLD.currency);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `account_log`
--

DROP TABLE IF EXISTS `account_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `account_log` (
  `pk` int(11) NOT NULL AUTO_INCREMENT,
  `table_action` enum('INSERT','UPDATE','DELETE') NOT NULL,
  `id` varchar(100) NOT NULL,
  `cash` decimal(20,5) DEFAULT NULL,
  `currency` varchar(3) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `account_log_pk_uindex` (`pk`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `broker`
--

DROP TABLE IF EXISTS `broker`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `broker` (
  `id` varchar(100) NOT NULL,
  `description` varchar(500) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `broker_id_uindex` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `execution`
--

DROP TABLE IF EXISTS `execution`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `execution` (
  `broker_id` varchar(100) NOT NULL,
  `broker_order_id` varchar(100) NOT NULL,
  `broker_execution_id` varchar(100) NOT NULL,
  `gateway_order_id` varchar(100) NOT NULL,
  `is_buy` tinyint(1) NOT NULL,
  `contract` varchar(50) NOT NULL,
  `quantity` int(11) NOT NULL,
  `price` decimal(20,5) NOT NULL,
  `leave_quantity` int(11) DEFAULT NULL,
  `commission` decimal(20,5) NOT NULL,
  `currency` varchar(10) NOT NULL,
  `execution_datetime` datetime NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `execution_pk` (`broker_id`,`broker_execution_id`),
  KEY `execution_broker_id_broker_order_id_index` (`broker_id`,`broker_order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `instrument`
--

DROP TABLE IF EXISTS `instrument`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `instrument` (
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `code` varchar(50) NOT NULL,
  `expiry` date NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`market`,`symbol`),
  CONSTRAINT `instruments_market_market_fk` FOREIGN KEY (`market`) REFERENCES `market` (`market`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `market`
--

DROP TABLE IF EXISTS `market`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market` (
  `market` varchar(10) NOT NULL,
  PRIMARY KEY (`market`),
  UNIQUE KEY `market_market_uindex` (`market`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `order_`
--

DROP TABLE IF EXISTS `order_`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `order_` (
  `session_id` varchar(100) NOT NULL,
  `order_id` int(25) NOT NULL,
  `parent_order_id` int(25) DEFAULT NULL,
  `broker_id` varchar(100) NOT NULL,
  `broker_order_id` varchar(100) NOT NULL,
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `type` enum('MKT','LMT','STP','STP_LMT') NOT NULL,
  `is_buy` tinyint(1) NOT NULL,
  `quantity` int(11) NOT NULL,
  `price` decimal(20,5) NOT NULL,
  `state` enum('NEW','PENDING','ACTIVE','CANCELLED','REJECTED','PARTICALLY_FILLED','FULLY_FILLED','INACTIVE') DEFAULT NULL,
  `filled_quantity` int(11) DEFAULT NULL,
  `remaining_quantity` int(11) DEFAULT NULL,
  `qualifier` enum('NONE','ALGO','STOP','PEGGED') DEFAULT NULL,
  `action` enum('ENTRY','EXIT','REENTRY','INCREASE','REDUCE','STOP_LOSS','MANUAL_STOP_LOSS','AMEND','CANCEL','MANUAL_ORDER','ROLL') DEFAULT NULL,
  `portfolio` varchar(100) NOT NULL,
  `strategy` varchar(100) DEFAULT NULL,
  `reference` varchar(100) DEFAULT NULL,
  `comment` varchar(1000) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`broker_id`,`broker_order_id`),
  UNIQUE KEY `order__broker_id_broker_order_id_uindex` (`broker_id`,`broker_order_id`),
  KEY `order__portfolio_id_fk` (`portfolio`),
  KEY `order__strategy_id_fk` (`strategy`),
  KEY `order_market_market_fk` (`market`),
  KEY `order_session_id_order_id_index` (`session_id`,`order_id`),
  CONSTRAINT `order__portfolio_id_fk` FOREIGN KEY (`portfolio`) REFERENCES `portfolio` (`id`),
  CONSTRAINT `order__strategy_id_fk` FOREIGN KEY (`strategy`) REFERENCES `strategy` (`id`),
  CONSTRAINT `order_market_market_fk` FOREIGN KEY (`market`) REFERENCES `market` (`market`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_insert_order
after INSERT on order_
for each row
begin
    insert into order_log (table_action, session_id, order_id, parent_order_id, broker_id, broker_order_id, market, symbol, type, is_buy, quantity, price, state, filled_quantity, remaining_quantity, qualifier, action, portfolio, strategy, reference, comment) values ('insert', NEW.session_id, NEW.order_id, NEW.parent_order_id, NEW.broker_id, NEW.broker_order_id, NEW.market, NEW.symbol, NEW.type, NEW.is_buy, NEW.quantity, NEW.price, NEW.state, NEW.filled_quantity, NEW.remaining_quantity, NEW.qualifier, NEW.action, NEW.portfolio, NEW.strategy, NEW.reference, NEW.comment);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_update_order
after UPDATE on order_
for each row
begin
        insert into order_log (table_action, session_id, order_id, parent_order_id, broker_id, broker_order_id, market, symbol, type, is_buy, quantity, price, state, filled_quantity, remaining_quantity, qualifier, action, portfolio, strategy, reference, comment) values ('update', NEW.session_id, NEW.order_id, NEW.parent_order_id, NEW.broker_id, NEW.broker_order_id, NEW.market, NEW.symbol, NEW.type, NEW.is_buy, NEW.quantity, NEW.price, NEW.state, NEW.filled_quantity, NEW.remaining_quantity, NEW.qualifier, NEW.action, NEW.portfolio, NEW.strategy, NEW.reference, NEW.comment);
    end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_delete_order
after DELETE on order_
for each row
begin
    insert into order_log (table_action, session_id, order_id, parent_order_id, broker_id, broker_order_id, market, symbol, type, is_buy, quantity, price, state, filled_quantity, remaining_quantity, qualifier, action, portfolio, strategy, reference, comment) values ('delete', OLD.session_id, OLD.order_id, OLD.parent_order_id, OLD.broker_id, OLD.broker_order_id, OLD.market, OLD.symbol, OLD.type, OLD.is_buy, OLD.quantity, OLD.price, OLD.state, OLD.filled_quantity, OLD.remaining_quantity, OLD.qualifier, OLD.action, OLD.portfolio, OLD.strategy, OLD.reference, OLD.comment);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `order_log`
--

DROP TABLE IF EXISTS `order_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `order_log` (
  `pk` int(25) NOT NULL AUTO_INCREMENT,
  `table_action` enum('INSERT','UPDATE','DELETE') NOT NULL,
  `order_id` int(25) NOT NULL,
  `parent_order_id` int(25) DEFAULT NULL,
  `broker_id` varchar(100) DEFAULT NULL,
  `broker_order_id` varchar(100) NOT NULL,
  `session_id` varchar(100) NOT NULL,
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `type` enum('MKT','LMT','STP','STP_LMT') NOT NULL,
  `is_buy` tinyint(1) NOT NULL,
  `quantity` int(11) NOT NULL,
  `price` decimal(20,5) NOT NULL,
  `state` enum('new','pending','active','cancelled','rejected','partically_filled','fully_filled','inactive') DEFAULT NULL,
  `filled_quantity` int(11) DEFAULT NULL,
  `remaining_quantity` int(11) DEFAULT NULL,
  `qualifier` enum('none','algo','stop','pegged') DEFAULT NULL,
  `portfolio` varchar(100) DEFAULT NULL,
  `action` enum('entry','exit','reentry','increase','reduce','stop_loss','manual_stop_loss','amend','cancel','manual_order','roll') DEFAULT NULL,
  `strategy` varchar(100) DEFAULT NULL,
  `reference` varchar(100) DEFAULT NULL,
  `comment` varchar(1000) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `order_log_pk_uindex` (`pk`)
) ENGINE=InnoDB AUTO_INCREMENT=13172 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `portfolio`
--

DROP TABLE IF EXISTS `portfolio`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `portfolio` (
  `id` varchar(100) NOT NULL,
  `account_id` varchar(100) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `portfolio_id_uindex` (`id`),
  KEY `portfolio_account_id_fk` (`account_id`),
  CONSTRAINT `portfolio_account_id_fk` FOREIGN KEY (`account_id`) REFERENCES `account` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `position`
--

DROP TABLE IF EXISTS `position`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `position` (
  `portfolio_id` varchar(100) NOT NULL,
  `strategy` varchar(100) NOT NULL,
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `position` int(11) NOT NULL,
  `avg_price` decimal(20,5) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`portfolio_id`,`strategy`),
  CONSTRAINT `position_portfolio_id_fk` FOREIGN KEY (`portfolio_id`) REFERENCES `portfolio` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_insert_position
after INSERT on position
for each row
begin
    insert into position_log (table_action, portfolio_id, strategy, market, symbol, position) values ('insert', NEW.portfolio_id, NEW.strategy, NEW.market, NEW.symbol, NEW.position);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_update_position
after UPDATE on position
for each row
begin
    insert into position_log (table_action, portfolio_id, strategy, market, symbol, position) values ('update', NEW.portfolio_id, NEW.strategy, NEW.market, NEW.symbol, NEW.position);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 trigger log_delete_position
after DELETE on position
for each row
begin
    insert into position_log (table_action, portfolio_id, strategy, market, symbol, position) values ('delete', OLD.portfolio_id, OLD.strategy, OLD.market, OLD.symbol, OLD.position);
end */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `position_by_entry`
--

DROP TABLE IF EXISTS `position_by_entry`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `position_by_entry` (
  `portfolio_id` varchar(100) NOT NULL,
  `strategy` varchar(100) NOT NULL,
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `position` int(11) NOT NULL,
  `avg_price` decimal(20,5) DEFAULT NULL,
  `session_id` varchar(100) NOT NULL,
  `order_id` int(25) NOT NULL,
  `state` enum('PENDING','FULLY_FILLED','EXITED') DEFAULT NULL,
  `order_reference` varchar(100) NOT NULL,
  `created` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`portfolio_id`,`strategy`,`market`,`symbol`,`session_id`,`order_id`),
  KEY `position_by_entry_market_fk` (`market`),
  KEY `position_by_entry_order_id_fk` (`session_id`,`order_id`),
  CONSTRAINT `position_by_entry_market_fk` FOREIGN KEY (`market`) REFERENCES `market` (`market`),
  CONSTRAINT `position_by_entry_order_id_fk` FOREIGN KEY (`session_id`, `order_id`) REFERENCES `order_` (`session_id`, `order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `position_log`
--

DROP TABLE IF EXISTS `position_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `position_log` (
  `pk` int(11) NOT NULL AUTO_INCREMENT,
  `table_action` enum('INSERT','UPDATE','DELETE') NOT NULL,
  `portfolio_id` varchar(100) NOT NULL,
  `strategy` varchar(100) NOT NULL,
  `market` varchar(10) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `position` int(11) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `position_log_id_uindex` (`pk`)
) ENGINE=InnoDB AUTO_INCREMENT=3830 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `session`
--

DROP TABLE IF EXISTS `session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `session` (
  `id` varchar(100) NOT NULL,
  `next_request_id` int(11) NOT NULL DEFAULT '0',
  `ip` varchar(100) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `session_id_uindex` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `strategy`
--

DROP TABLE IF EXISTS `strategy`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `strategy` (
  `id` varchar(100) NOT NULL,
  `description` varchar(500) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `strategy_id_index` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50112 SET @disable_bulk_load = IF (@is_rocksdb_supported, 'SET SESSION rocksdb_bulk_load = @old_rocksdb_bulk_load', 'SET @dummy_rocksdb_bulk_load = 0') */;
/*!50112 PREPARE s FROM @disable_bulk_load */;
/*!50112 EXECUTE s */;
/*!50112 DEALLOCATE PREPARE s */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-10-09 13:01:23

--
-- Table structure for table `operation`
--

DROP TABLE IF EXISTS `operation`;
CREATE TABLE `operation` (
  `portfolio_id` varchar(100) NOT NULL,
  `strategy` varchar(100) NOT NULL,
  `action` varchar(100) NOT NULL,
  `position` int(11) NOT NULL,
  `price` decimal(20,5) DEFAULT NULL,
  `identity` varchar(100) DEFAULT NULL,
  `order_reference` varchar(100) NOT NULL,
  `created` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`,`portfolio_id`,`strategy`,`order_reference`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=latin1