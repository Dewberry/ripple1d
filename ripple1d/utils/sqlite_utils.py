"""Utils for working with sqlite databases."""

import os
import sqlite3

import pandas as pd

from ripple1d.ras import RasManager


def create_db_and_table(db_name: str, table_name: str):
    """Create sqlite database and table."""
    os.makedirs(os.path.dirname(os.path.abspath(db_name)), exist_ok=True)
    sql_query = f"""
        CREATE TABLE {table_name}(
            reach_id INTEGER,
            ds_depth REAL,
            ds_wse REAL,
            us_flow INTEGER,
            us_depth REAL,
            us_wse REAL,
            boundary_condition TEXT, -- [kwse, nd]
            plan_suffix TEXT,
            map_exist BOOL CHECK(map_exist IN (0, 1)),
            UNIQUE(reach_id, us_flow, ds_wse, boundary_condition, plan_suffix)
        )
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(sql_query)
    conn.commit()
    conn.close()


def insert_data(
    db_name: str, table_name: str, data: pd.DataFrame, plan_suffix: str, missing_grids: list, boundary_condition: str
):
    """Insert data into the sqlite database."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    for row in data.itertuples():
        if boundary_condition == "kwse":
            if f'f_{int(row.us_flow)}-z_{str(row.ds_wse).replace(".","_")}' in missing_grids:
                map_exist = 0
            else:
                map_exist = 1
        elif boundary_condition == "nd":
            if str(int(row.us_flow)) in missing_grids:
                map_exist = 0
            else:
                map_exist = 1
        else:
            raise ValueError(
                f"Could not detemine boundary condition type for {boundary_condition}; expected kwse or nd"
            )

        c.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (reach_id, ds_depth, ds_wse, us_flow, us_depth, us_wse, boundary_condition,  plan_suffix, map_exist)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                int(row.reach_id),
                round(float(row.ds_depth), 1),
                round(float(row.ds_wse), 1),
                round(float(row.us_flow), 1),
                round(float(row.us_depth), 1),
                round(float(row.us_wse), 1),
                str(boundary_condition),
                str(plan_suffix),
                str(map_exist),
            ),
        )

    conn.commit()
    conn.close()


def parse_stage_flow(wses: pd.DataFrame) -> pd.DataFrame:
    """Parse flow and control by stage from profile names."""
    wses_t = wses.T
    wses_t.reset_index(inplace=True)
    wses_t[["us_flow", "ds_wse"]] = wses_t["index"].str.split("-", n=1, expand=True)
    wses_t.drop(columns="index", inplace=True)
    wses_t["us_flow"] = wses_t["us_flow"].str.lstrip("f_").astype(float)
    wses_t["ds_wse"] = wses_t["ds_wse"].str.lstrip("z_")
    wses_t["ds_wse"] = wses_t["ds_wse"].str.replace("_", ".").astype(float)

    return wses_t


def zero_depth_to_sqlite(
    rm: RasManager,
    plan_name: str,
    plan_suffix: str,
    nwm_id: str,
    missing_grids: list,
    database_path: str,
    table_name: str,
):
    """Export zero depth (normal depth) results to sqlite."""
    # set the plan
    rm.plan = rm.plans[plan_name]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # get river-reach-rs
    us_river_reach_rs = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.river_reach_rs
    ds_river_reach_rs = rm.plan.geom.rivers[nwm_id][nwm_id].ds_xs.river_reach_rs

    wses_t = wses.T
    wses_t["us_flow"] = wses_t.index
    wses_t["ds_depth"] = 0
    df = wses_t.loc[:, [us_river_reach_rs, "us_flow", "ds_depth"]]
    df.rename(columns={us_river_reach_rs: "us_wse"}, inplace=True)

    # convert elvation to stage for upstream cross section
    us_thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.thalweg
    df["us_depth"] = df["us_wse"] - us_thalweg

    # convert elvation to stage for downstream cross section
    ds_df = wses_t.loc[:, [ds_river_reach_rs, "us_flow", "ds_depth"]]
    ds_df.rename(columns={ds_river_reach_rs: "us_wse"}, inplace=True)
    ds_thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].ds_xs.thalweg

    df["ds_wse"] = ds_df["us_wse"]
    df["ds_depth"] = df["ds_wse"] - ds_thalweg

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    insert_data(database_path, table_name, df, plan_suffix, missing_grids, boundary_condition="nd")


def rating_curves_to_sqlite(
    rm: RasManager,
    plan_name: str,
    plan_suffix: str,
    nwm_id: str,
    missing_grids: list,
    database_path: str,
    table_name: str,
):
    """Export rating curves to sqlite."""
    # set the plan
    if plan_name not in rm.plans:
        return
    rm.plan = rm.plans[plan_name]

    # read in flow/wse
    wses, flows = rm.plan.read_rating_curves()

    # parse applied stage and flow from profile names
    wses = parse_stage_flow(wses)

    # get river-reach-rs id
    river_reach_rs = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.river_reach_rs

    # get subset of results for this cross section
    df = wses[["us_flow", "ds_wse", river_reach_rs]].copy()

    # rename columns
    df.rename(columns={river_reach_rs: "us_wse"}, inplace=True)

    # add control id
    df["reach_id"] = [nwm_id] * len(df)

    # convert elevation to stage
    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].us_xs.thalweg
    df["us_depth"] = df["us_wse"] - thalweg

    thalweg = rm.plan.geom.rivers[nwm_id][nwm_id].ds_xs.thalweg
    df["ds_depth"] = df["ds_wse"] - thalweg

    insert_data(database_path, table_name, df, plan_suffix, missing_grids, boundary_condition="kwse")


def create_non_spatial_table(gpkg_path: str, metadata: dict) -> None:
    """Create the metadata table in the geopackage."""
    with sqlite3.connect(gpkg_path) as conn:
        string = ""
        curs = conn.cursor()
        curs.execute("DROP TABLE IF Exists metadata")
        curs.execute(f"CREATE TABLE IF NOT EXISTS metadata (key, value);")
        curs.close()

    with sqlite3.connect(gpkg_path) as conn:
        curs = conn.cursor()
        keys, vals = "", ""
        for key, val in metadata.items():

            if val:
                keys += f"{key},"
                vals += f"{val.replace(',','_')}, "
                curs.execute(f"INSERT INTO metadata (key,value) values (?,?);", (key, val))

        curs.execute("COMMIT;")
        curs.close()
    return None
