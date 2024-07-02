"""Utils for working with sqlite databases."""

import os
import sqlite3

import pandas as pd

from ripple.consts import RIPPLE_VERSION
from ripple.ras import RasManager


def create_db_and_table(db_name: str, table_name: str):
    """Create sqlite database and table."""
    if os.path.exists(db_name):
        os.remove(db_name)

    sql_query = f"""
        CREATE TABLE {[table_name]}(
            control_by_reach_depth REAL,
            control_by_reach_wse REAL,
            flow REAL,
            depth REAL,
            wse Real,
            reach_id INTEGER,
            ripple_version TEXT,
            UNIQUE(flow, reach_id, control_by_reach_depth)
        )
    """
    os.makedirs(os.path.dirname(os.path.abspath(db_name)), exist_ok=True)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(sql_query)
    conn.commit()
    conn.close()


def insert_data(db_name: str, table_name: str, data: pd.DataFrame, ripple_version: str = RIPPLE_VERSION):
    """Insert data into the sqlite database."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    for row in data.itertuples():

        c.execute(
            f"""
            INSERT OR REPLACE INTO {[table_name]} (control_by_reach_depth, control_by_reach_wse,flow, depth, wse, reach_id,ripple_version)
            VALUES ( ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                float(row.control_by_reach_depth),
                float(row.control_by_reach_wse),
                float(row.flow),
                float(row.depth),
                float(row.wse),
                int(row.reach_id),
                ripple_version,
            ),
        )

    conn.commit()
    conn.close()


def parse_stage_flow(wses: pd.DataFrame) -> pd.DataFrame:
    """Parse flow and control by stage from profile names."""
    wses_t = wses.T
    wses_t.reset_index(inplace=True)
    wses_t[["flow", "control_by_reach_wse"]] = wses_t["index"].str.split("-", expand=True)
    wses_t.drop(columns="index", inplace=True)
    wses_t["flow"] = wses_t["flow"].str.lstrip("f_").astype(float)
    wses_t["control_by_reach_wse"] = wses_t["control_by_reach_wse"].str.lstrip("z_")
    wses_t["control_by_reach_wse"] = wses_t["control_by_reach_wse"].str.replace("_", ".").astype(float)

    return wses_t


def zero_depth_to_sqlite(rm: RasManager, plan_name: str, nwm_id: str, missing_grids_nd: list):
    """Export zero depth (normal depth) results to sqlite."""
    database_path = os.path.join(rm.ras_project._ras_dir, "output", rm.ras_project._ras_project_basename + ".db")
    table = rm.ras_project._ras_project_basename

    # set the plan
    rm.plan = rm.plans[plan_name]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()
    if missing_grids_nd:
        wses.drop(columns=missing_grids_nd, inplace=True)
        flows.drop(columns=missing_grids_nd, inplace=True)

    # get river-reach-rs
    river_reach_rs = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.river_reach_rs

    wses_t = wses.T
    wses_t["flow"] = wses_t.index
    wses_t["control_by_reach_depth"] = 0
    df = wses_t.loc[:, [river_reach_rs, "flow", "control_by_reach_depth"]]
    df.rename(columns={river_reach_rs: "wse"}, inplace=True)

    # convert elevation to stage
    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.thalweg
    df["depth"] = df["wse"] - thalweg

    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].ds_xs.thalweg
    df["control_by_reach_wse"] = thalweg

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    insert_data(database_path, table, df)


def rating_curves_to_sqlite(rm: RasManager, plan_name: str, nwm_id: str, missing_grids_kwse: list):
    """Export rating curves to sqlite."""
    # create dabase and table
    database_path = os.path.join(rm.ras_project._ras_dir, "output", rm.ras_project._ras_project_basename + ".db")
    table = rm.ras_project._ras_project_basename

    create_db_and_table(database_path, table)

    # set the plan
    if plan_name not in rm.plans:
        return
    rm.plan = rm.plans[plan_name]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()
    if missing_grids_kwse:
        wses.drop(columns=missing_grids_kwse, inplace=True)
        flows.drop(columns=missing_grids_kwse, inplace=True)

    # parse applied stage and flow from profile names
    wses = parse_stage_flow(wses)

    # get river-reach-rs id
    river_reach_rs = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.river_reach_rs

    # get subset of results for this cross section
    df = wses[["flow", "control_by_reach_wse", river_reach_rs]].copy()

    # rename columns
    df.rename(columns={river_reach_rs: "wse"}, inplace=True)

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    # convert elevation to stage
    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.thalweg
    df["depth"] = df["wse"] - thalweg

    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].ds_xs.thalweg
    df["control_by_reach_depth"] = df["control_by_reach_wse"] - thalweg

    insert_data(database_path, table, df)
