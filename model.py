#!/usr/bin/env python3
import logging
from datetime import datetime

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance float,
        name TEXT,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_date (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    )
"""

CREATE_TRADES_BY_ACCOUNT_TYPE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_type (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, trade_id)
    )
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_symbol (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), symbol, trade_id)
    )
"""

SELECT_USER_ACCOUNTS = """
    SELECT *
    FROM accounts_by_user
    WHERE username = ?
"""
SELECT_POSITIONS = """
    SELECT *
    FROM positions_by_account
    WHERE account = ?
    order by symbol
"""
SELECT_TRANSACTIONS_BY_DATE = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_date
    WHERE account = ?
    and dateof(trade_id) >= todate(?)
    and dateof(trade_id) <= todate(?)
"""

SELECT_TRANSACTIONS_BY_ACCOUNT = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_date
    WHERE account = ?
"""

SELECT_TRANSACTIONS_BY_TYPE = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_type
    WHERE account = ?
    and type = ?
"""
SELECT_TRANSACTIONS_BY_SYMBOL = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_symbol
    WHERE account = ?
    and symbol = ?
"""

SELECT_TRANSACTIONS_BY_ACCOUNT_DATE = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_date
    WHERE account = ?
    and trade_id > maxTimeuuid(?)
    and trade_id < minTimeuuid(?)
"""

SELECT_TRANSACTIONS_BY_TYPE_DATE = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_type
    WHERE account = ?
    and type = ?
    and trade_id > maxTimeuuid(?)
    and trade_id < minTimeuuid(?)
"""
SELECT_TRANSACTIONS_BY_SYMBOL_DATE = """
    SELECT account,dateof(trade_id) as trade_id,amount,price,shares,symbol,type
    FROM trades_by_symbol
    WHERE account = ?
    and symbol = ?
    and trade_id > maxTimeuuid(?)
    and trade_id < minTimeuuid(?)
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TYPE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_SYMBOL_TABLE)

def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")

def get_positions(session):
    option = input('Enter your account: ')
    stmt = session.prepare(SELECT_POSITIONS)
    rows = session.execute(stmt, [option])
    for row in rows:
        print(f"=== Account: {row.account} ===")
        print(f"- Symbol: {row.symbol}")
        print(f"- Quantity: {row.quantity}")

def get_transactions(session,option):
    account = input('Enter account: ')
    array = [account]
    if option == 'account':
        stmt = session.prepare(SELECT_TRANSACTIONS_BY_ACCOUNT)
    if option == 'type':
        type = input('Buy or Sell: ')
        stmt = session.prepare(SELECT_TRANSACTIONS_BY_TYPE)
        array.append(type)
    if option == 'symbol':
        symbol = input('Enter instrument symbol: ')
        stmt = session.prepare(SELECT_TRANSACTIONS_BY_SYMBOL)
        array.append(symbol)

    rows = session.execute(stmt, array)
    for row in rows:
        print(f"=== Account: {row.account} ===")
        print(f"- Date: {row.trade_id}")
        print(f"- Type: {row.type}")
        print(f"- Symbol: {row.symbol}")
        print(f"- Shares: {row.shares}")
        print(f"- Price: $ {round(row.price,2)}")
        print(f"- Amount: {round(row.amount,2)}")

    filter = input('Would you like to filter the results by date (Y/N)?: ')
    if filter == 'Y':
        start_date = datetime.strptime(input("Enter Start Date (YYYY-MM-DD):"),'%Y-%m-%d')
        end_date = datetime.strptime(input("Enter End Date (YYYY-MM-DD):"),'%Y-%m-%d')

        array.append(start_date)
        array.append(end_date)
        if option == 'account':
            stmt = session.prepare(SELECT_TRANSACTIONS_BY_ACCOUNT_DATE)
        if option == 'type':
            stmt = session.prepare(SELECT_TRANSACTIONS_BY_TYPE_DATE)
        if option == 'symbol':
            stmt = session.prepare(SELECT_TRANSACTIONS_BY_SYMBOL_DATE)

        rows = session.execute(stmt, array)
        for row in rows:
            print(f"=== Account: {row.account} ===")
            print(f"- Date: {row.trade_id}")
            print(f"- Type: {row.type}")
            print(f"- Symbol: {row.symbol}")
            print(f"- Shares: {row.shares}")
            print(f"- Price: $ {round(row.price,2)}")
            print(f"- Amount: {round(row.amount,2)}")




