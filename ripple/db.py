import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

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

    def create_table(self, table: str, fields: list[tuple]):
        with psycopg2.connect(self.__conn_string()) as connection:
            fields_string = ""
            for field, data_type in fields:
                fields_string += f"{field} {data_type}"
            cursor = connection.cursor()
            cursor.execute(sql.SQL(f"CREATE TABLE cases.{table} ({fields_string})"))

    def read_cases(self, table: str):
        with psycopg2.connect(self.__conn_string()) as connection:
            cursor = connection.cursor()
            cursor.execute(sql.SQL(f"SELECT mip_case, key, crs FROM cases.{table}"))
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
            cursor.exectue(insert_query, values)
            connection.commit()

    def update_case_status(self, mip_group, mip_case, key, status, exc, traceback):
        with psycopg2.connect(self.__conn_string()) as connection:
            cursor = connection.cursor()
            insert_query = sql.SQL(
                """
                INSERT INTO cases.processing(mip_group, mip_case, s3_key, gpkg_complete, gpkg_exc, gpkg_traceback) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (mip_group, mip_case, s3_key)
                DO UPDATE SET
                gpkg_complete = EXCLUDED.gpkg_complete,
                gpkg_exc = EXCLUDED.gpgk_exc
                gpkg_traceback = EXCLUDED.gpkg_traceback
                """
            )
            cursor.execute(insert_query, (mip_group, mip_case, key, status, exc, traceback))
            connection.commit()


def temp_func(self, result):
    with psycopg2.connect(self.__conn_string()) as connection:
        cursor = connection.cursor()
        insert_sql = sql.SQL(
            """
        INSERT INTO cases.inferred_crs (mip_group,mip_case,s3_key,crs,ratio_of_best_crs)
        VALUES (%s,%s,%s,%s,%s)
        """
        )
        cursor.execute(insert_sql, ("A", *results))


# db = PGFim()
# results = db.read_cases("inferred_crs_a")

# for result in results[0:5]:
#     mip_case, s3_key = result[0], result[1]
#     print(s3_key)
#     success = True
#     traceback = "Testing  - No traceback"
#     db.update_case_status("a", mip_case, s3_key, success, traceback)

db = PGFim()
results = db.read_cases("inferred_crs_a")
for result in results:
    db.temp_func(result)
