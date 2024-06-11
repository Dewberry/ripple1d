import os
import sqlite3

import pandas as pd

from .ras2 import RasManager


def create_db_and_table(db_name: str, table_name: str):
    """
    Create sqlite database and table
    """
    if os.path.exists(db_name):
        os.remove(db_name)

    sql_query = f"""
        CREATE TABLE {[table_name]}(
            control_by_reach_stage REAL,
            flow REAL,
            stage REAL,
            reach_id INTEGER,
            UNIQUE(flow, reach_id, control_by_reach_stage)
        )
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(sql_query)
    conn.commit()
    conn.close()


def insert_data(db_name: str, table_name: str, data: pd.DataFrame):
    """
    Insert data into the sqlite database
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    for row in data.itertuples():

        c.execute(
            f"""
            INSERT OR REPLACE INTO {[table_name]} (control_by_reach_stage, flow, stage,reach_id)
            VALUES ( ?, ?, ?, ?)
        """,
            (float(row.control_by_reach_stage), float(row.flow), float(row.stage), int(row.reach_id)),
        )

    conn.commit()
    conn.close()


def parse_stage_flow(wses: pd.DataFrame) -> pd.DataFrame:
    """Parse flow and control by stage from profile names"""
    wses_t = wses.T
    wses_t.reset_index(inplace=True)
    wses_t[["flow", "control_by_reach_stage"]] = wses_t["index"].str.split("-", expand=True)
    wses_t.drop(columns="index", inplace=True)
    wses_t["flow"] = wses_t["flow"].str.lstrip("f_").astype(float)
    wses_t["control_by_reach_stage"] = wses_t["control_by_reach_stage"].str.lstrip("z_")
    wses_t["control_by_reach_stage"] = wses_t["control_by_reach_stage"].str.replace("_", ".").astype(float)

    return wses_t


def zero_depth_to_sqlite(rm: RasManager, nwm_id: str):

    database_path = os.path.join(rm.ras_project._ras_dir, "output", rm.ras_project._ras_project_basename + ".db")
    table = rm.ras_project._ras_project_basename

    # set the plan
    rm.plan = rm.plans[str(nwm_id) + "_nd"]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # get river-reach-rs
    xs_gdf = rm.plan.geom.xs_gdf
    rs = xs_gdf["river_station"].max()
    river_reach_rs = f"{nwm_id} {nwm_id} {rs}".rstrip("0")

    wses_t = wses.T
    wses_t["flow"] = wses_t.index
    wses_t["control_by_reach_stage"] = 0
    df = wses_t.loc[:, [river_reach_rs, "flow", "control_by_reach_stage"]]
    df.rename(columns={river_reach_rs: "stage"}, inplace=True)

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    insert_data(database_path, table, df)


def rating_curves_to_sqlite(rm: RasManager, nwm_id: str, nwm_data: dict):
    """Export rating curves to sqlite"""
    # create dabase and table
    database_path = os.path.join(rm.ras_project._ras_dir, "output", rm.ras_project._ras_project_basename + ".db")
    table = rm.ras_project._ras_project_basename

    create_db_and_table(database_path, table)

    # set the plan
    rm.plan = rm.plans[str(nwm_id) + "_kwse"]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # parse applied stage and flow from profile names
    wses = parse_stage_flow(wses)

    # get river-reach-rs id
    xs_gdf = rm.plan.geom.xs_gdf
    rs = xs_gdf["river_station"].max()
    river_reach_rs = f"{nwm_id} {nwm_id} {rs}".rstrip("0")

    # get subset of results for this cross section
    df = wses[["flow", "control_by_reach_stage", river_reach_rs]].copy()

    # rename columns
    df.rename(columns={river_reach_rs: "stage"}, inplace=True)

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    # convert elevation to stage
    thalweg = xs_gdf.loc[xs_gdf["river_station"] == rs, "thalweg"][0]
    df.loc[:, "stage"] = df["stage"] - thalweg

    insert_data(database_path, table, df)
