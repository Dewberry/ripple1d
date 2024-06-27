import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

from ripple.stacio.s3_utils import list_keys
from ripple.utils import get_sessioned_s3_client

load_dotenv()


class PGFim:
    def __init__(self):
        self.dbuser = os.getenv("DBUSER")
        self.dbpass = os.getenv("DBPASS")
        self.dbhost = os.getenv("DBHOST")
        self.dbport = os.getenv("DBPORT")
        self.dbname = os.getenv("DBNAME")

    def __conn_string(self):
        conn_string = f"dbname='{self.dbname}' user='{self.dbuser}' password='{self.dbpass}' host='{self.dbhost}' port='{self.dbport}'"
        return conn_string

    def create_table(self, table: str, fields: list[str]):
        with psycopg2.connect(self.__conn_string()) as connection:
            fields_string = ""
            for field, data_type in fields:
                fields_string += f"{field} {data_type}"
            cursor = connection.cursor()
            cursor.execute(sql.SQL(f"CREATE TABLE cases.{table} ({fields_string})"))

    def read_cases(self, table: str, fields: list[str], mip_group: str, optional_condition=""):
        with psycopg2.connect(self.__conn_string()) as connection:
            cursor = connection.cursor()
            fields_str = ""
            for field in fields:
                fields_str += f"{field}, "
            sql_query = sql.SQL(
                f"SELECT {fields_str.rstrip(', ')} FROM cases.{table} WHERE mip_group='{mip_group}' {optional_condition};"
            )
            cursor.execute(sql_query)
            return cursor.fetchall()

    def update_table(self, table: str, fields: tuple, values: tuple):
        with psycopg2.connect(self.__conn_string()) as connection:
            cursor = connection.cursor()
            insert_query = sql.SQL(
                f"""
                INSERT INTO cases.{table}{fields}
                VALUES ({tuple("%s" for i in range(len(fields)))})
                """
            )
            cursor.execute(insert_query, values)
            connection.commit()

    def update_case_status(
        self, mip_group: str, mip_case: str, key: str, status: bool, exc: str, traceback: str, process: str
    ):
        with psycopg2.connect(self.__conn_string()) as connection:
            cursor = connection.cursor()
            insert_query = sql.SQL(
                f"""
                INSERT INTO cases.processing(s3_key,mip_group, case_id, {process}_complete, {process}_exc, {process}_traceback) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (s3_key)
                DO UPDATE SET
                {process}_complete = EXCLUDED.{process}_complete,
                {process}_exc = EXCLUDED.{process}_exc,
                {process}_traceback = EXCLUDED.{process}_traceback
                """
            )
            cursor.execute(insert_query, (key, mip_group, mip_case, status, exc, traceback))
            connection.commit()


def read_case_db(cases_db_path: str, table_name: str):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT key, crs FROM {table_name} ")
        return cursor.fetchall()


def add_columns(cases_db_path: str, table_name: str, columns: list[str]):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()
        existing_columns = cursor.execute(f"""SELECT * FROM {table_name}""")
        for column in columns:
            if column in [c[0] for c in existing_columns.description]:
                cursor.execute(f"ALTER TABLE {table_name} DROP {column}")
                connection.commit()
        cursor.execute(f"ALTER TABLE {table_name} ADD {column} TEXT")
        connection.commit()


def insert_data(cases_db_path: str, table_name: str, data):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()
        for key, val in data.items():
            cursor.execute(
                f"""INSERT OR REPLACE INTO {table_name} (exc, tb, gpkg, crs, key) VALUES (?, ?, ?, ?, ?)""",
                (val["exc"], val["tb"], val["gpkg"], val["crs"], key),
            )
        connection.commit()


def create_table(cases_db_path: str, table_name: str):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()
        res = cursor.execute(f"SELECT name FROM sqlite_master WHERE name='{table_name}'")
        if res.fetchone():
            cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()

        cursor.execute(f"""Create Table {table_name} (key Text, crs Text, gpkg Text, exc Text, tb Text)""")
        connection.commit()


def create_tx_ble_db(s3_prefix: str, crs: int, db_path: str, bucket: str = "fim"):

    if os.path.exists(db_path):
        os.remove(db_path)
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("""create table tx_ble_crs_A (key Text, crs Text)""")

    client = get_sessioned_s3_client()
    keys = list_keys(client, bucket, s3_prefix, ".prj")
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        for i, key in enumerate(keys):

            cursor.execute(
                """Insert or replace into tx_ble_crs_A (key,crs) values (?, ?)""",
                (key, f"EPSG:{crs}"),
            )
            connection.commit()
