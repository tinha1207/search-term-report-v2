from dotenv import load_dotenv
import mysql.connector
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import urllib
from clickhouse_driver import connect as ch_connect


load_dotenv()


def create_ods_clickhouse_engine():
    conn_string = f'clickhouse://{os.environ["CH_USER"]}:{os.environ["CH_PASS"]}@{os.environ["CH_HOST"]}:{os.environ["CH_PORT"]}/{os.environ["CH_DATABASE"]}'
    engine = create_engine(conn_string)
    print("success")
    return engine


def connect_ods_clickhouse():
    success_str = "Connection successful."
    conn = ch_connect(
        user=os.environ["CH_USER"],
        password=os.environ["CH_PASS"],
        host=os.environ["CH_HOST"],
        database=os.environ["CH_DATABASE"],
        port=os.environ["CH_PORT"],
        # connect_timeout=11, send_receive_timeout=22, sync_request_timeout=33
    )
    print(success_str)
    return conn


def connect_sem_dev():
    success_str = "Connection successful."
    conn = mysql.connector.connect(
        user="tha", password="dollars", host="localhost", database="dev", port=3306
    )
    print(success_str)
    return conn


def connect_ods():
    success_str = "Connection successful."
    conn = mysql.connector.connect(
        user=os.environ["USER"],
        password=os.environ["PASS"],
        host=os.environ["HOST"],
        database=os.environ["DATABASE"],
    )
    print(success_str)
    return conn


def create_ods_engine():
    url = urllib.parse.quote_plus(os.environ["PASS"])
    print(url)
    conn_string = f'mysql+mysqlconnector://{os.environ["USER"]}:{url}@{os.environ["HOST"]}:{os.environ["PORT"]}/{os.environ["DATABASE"]}'
    engine = create_engine(conn_string)
    return engine


def main():
    engine = create_ods_clickhouse_engine()


if __name__ == "__main__":
    main()
