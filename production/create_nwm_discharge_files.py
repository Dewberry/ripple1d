""" duckdb is 10x faster than pandas for this script
"""

import duckdb

parquet_file = r"D:\\Users\abdul.siddiqui\\workbench\\projects\\ripple_1d_outputs\\nwm_flowlines.parquet"
discharge_columns = ["f2year", "f5year", "f10year", "f25year", "f50year", "f100year"]
output_csv_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\scenarios"


con = duckdb.connect()

con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{parquet_file}')")

for col in discharge_columns:
    output_csv = f"{output_csv_dir}\\{col.replace("f", "flows_")}.csv"
    query = f"""
        COPY (SELECT id AS nwm_feature_id, {col} AS discharge FROM data)
        TO '{output_csv}'
        (FORMAT CSV, HEADER TRUE);
    """
    con.execute(query)
    print(f"Created {output_csv}")

print("All CSV files have been created.")

con.close()
