-- Snowflake Setup for Solana Kafka Pipeline
-- Run this entire script as ACCOUNTADMIN before starting the Kafka stack

USE ROLE ACCOUNTADMIN;

-- Database and Schema
CREATE DATABASE IF NOT EXISTS SOLANA_DEX;
USE DATABASE SOLANA_DEX;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Warehouse
-- Use existing or create a small one for the connector
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Role for Kafka Connector
CREATE ROLE IF NOT EXISTS KAFKA_ROLE;

GRANT USAGE ON DATABASE SOLANA_DEX TO ROLE KAFKA_ROLE;
GRANT USAGE ON SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;
GRANT CREATE TABLE ON SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;
GRANT CREATE STAGE ON SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;
GRANT CREATE PIPE ON SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE KAFKA_ROLE;

-- Allow connector to modify tables (add RECORD_METADATA column)
GRANT MODIFY ON SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;

-- Grant permissions on all future tables the connector will create
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA SOLANA_DEX.PUBLIC TO ROLE KAFKA_ROLE;

-- User for Kafka Connector
CREATE USER IF NOT EXISTS KAFKA_USER
    PASSWORD = 'Password123!'  -- You can set a password but we'll use key-pair auth
    DEFAULT_ROLE = KAFKA_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE KAFKA_ROLE TO USER KAFKA_USER;

-- Register Public Key for Key-Pair Auth
-- After generating your key pair (see README.md), run this command with your public key:
-- ALTER USER KAFKA_USER SET RSA_PUBLIC_KEY='<contents of snowflake_public.pub minus header/footer>';

-- Tables
-- Tables will be AUTO-CREATED by the Snowflake Kafka Connector when it starts.
-- The connector will create tables with the following schema plus RECORD_METADATA:
--
-- BUYS table schema (auto-created):
--   - SIGNATURE, SIGNER, TYPE, TOKEN_MINT, TOKEN_AMOUNT, SOL_AMOUNT
--   - SLOT, BLOCK_TIME, INGESTED_AT
--   - RECORD_METADATA (auto-added by connector for Kafka tracking)
--
-- SELLS table schema (auto-created):
--   - Same as BUYS
--
-- FAILED_TXS table schema (auto-created):
--   - SIGNATURE, RAW_LOG, ERROR_MSG, FAILED_AT
--   - RECORD_METADATA (auto-added by connector)

-- Verify
SHOW DATABASES LIKE 'SOLANA_DEX';
SHOW SCHEMAS IN DATABASE SOLANA_DEX;
SHOW USERS LIKE 'KAFKA_USER';
SHOW ROLES LIKE 'KAFKA_ROLE';
SHOW GRANTS TO ROLE KAFKA_ROLE;