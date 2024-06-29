import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()


class PGFim:
    """Class to interact with the FIM database."""

    def __init__(self):
        self.dbuser = os.getenv("DBUSER")
        self.dbpass = os.getenv("DBPASS")
        self.dbhost = os.getenv("DBHOST")
        self.dbport = os.getenv("DBPORT")
        self.dbname = os.getenv("DBNAME")

    def __conn_string(self):
        conn_string = f"dbname='{self.dbname}' user='{self.dbuser}' password='{self.dbpass}' host='{self.dbhost}' port='{self.dbport}'"
        return conn_string

    def read_cases(self, table: str, fields: list[str], mip_group: str, optional_condition: str):
        """Read cases from the cases schema."""
        if optional_condition not in ["gpkg", "stac", "conflation"]:
            raise ValueError(f"optional_condition must be one of {optional_condition} or None")

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

    def update_case_status(
        self, mip_group: str, mip_case: str, key: str, status: bool, exc: str, traceback: str, process: str
    ):
        """Update the status of a table in the cases schema."""
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
