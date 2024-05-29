import os
import sqlite3

import pandas as pd
from ras import Ras


def create_db_and_table(db_name: str, table_name: str):
    """
    Create sqlite database and table
    """
    if os.path.exists(db_name):
        os.remove(db_name)

    sql_query = f"""
        CREATE TABLE {[table_name]}(
            control_by_node_stage REAL,
            flow REAL,
            stage REAL,
            node_id INTEGER,
            UNIQUE(flow, node_id, control_by_node_stage)
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
            INSERT OR REPLACE INTO {[table_name]} (control_by_node_stage, flow, stage,node_id)
            VALUES ( ?, ?, ?, ?)
        """,
            (float(row.control_by_node_stage), float(row.flow), float(row.stage), int(row.node_id)),
        )

    conn.commit()
    conn.close()


def parse_stage_flow(wses: pd.DataFrame) -> pd.DataFrame:
    """Parse flow and control by stage from profile names"""
    wses_t = wses.T
    wses_t.reset_index(inplace=True)
    wses_t[["flow", "control_by_node_stage"]] = wses_t["index"].str.split("-", expand=True)
    wses_t.drop(columns="index", inplace=True)
    wses_t["flow"] = wses_t["flow"].str.lstrip("f_").astype(float)
    wses_t["control_by_node_stage"] = wses_t["control_by_node_stage"].str.lstrip("z_")
    wses_t["control_by_node_stage"] = wses_t["control_by_node_stage"].str.replace("_", ".").astype(float)

    return wses_t


def zero_depth_to_sqlite(r: Ras):

    database_path = os.path.join(r.postprocessed_output_folder, r.ras_project_basename + ".db")
    table = r.ras_project_basename

    for branch_id, branch_data in r.nwm_dict.items():

        # set the plan
        r.plan = r.plans[str(branch_id) + "_nd"]

        # read in flow/wse
        rc = r.plan.read_rating_curves()
        wses, flows = rc.values()

        # create rating curve for each  node:
        node_data = branch_data["intermediate_data"] + [branch_data["upstream_data"]]

        for nd in node_data:
            # get river-reach-rs id for the intermediate node
            river = nd["river"]
            reach = nd["reach"]
            rs = nd["xs_id"]
            river_reach_rs = f"{river} {reach} {rs}"

            wses_t = wses.T
            wses_t["flow"] = wses_t.index
            wses_t["control_by_node_stage"] = 0
            df = wses_t.loc[:, [river_reach_rs, "flow", "control_by_node_stage"]]
            df.rename(columns={river_reach_rs: "stage"}, inplace=True)

            # add control id
            df["node_id"] = [nd["node_id"]] * len(df)

            insert_data(database_path, table, df)


def rating_curves_to_sqlite(r: Ras):
    """Export rating curves to sqlite"""
    # create dabase and table
    database_path = os.path.join(r.postprocessed_output_folder, r.ras_project_basename + ".db")
    table = r.ras_project_basename

    create_db_and_table(database_path, table)

    # df_list = []
    for branch_id, branch_data in r.nwm_dict.items():

        # set the plan
        r.plan = r.plans[str(branch_id) + "_kwse"]

        # read in flow/wse
        rc = r.plan.read_rating_curves()
        wses, flows = rc.values()

        # parse applied stage and flow from profile names
        wses = parse_stage_flow(wses)

        # create rating curve for each  node:
        node_data = branch_data["intermediate_data"] + [branch_data["upstream_data"]]

        for nd in node_data:

            # get river-reach-rs id for the intermediate node
            river = nd["river"]
            reach = nd["reach"]
            rs = nd["xs_id"]
            river_reach_rs = f"{river} {reach} {rs}"

            # get subset of results for this cross section
            df = wses[["flow", "control_by_node_stage", river_reach_rs]].copy()

            # rename columns
            df.rename(columns={river_reach_rs: "stage"}, inplace=True)

            # add control id
            df["node_id"] = [nd["node_id"]] * len(df)

            # convert elevation to stage
            thalweg = nd["min_elevation"]
            df.loc[:, "stage"] = df["stage"] - thalweg

            insert_data(database_path, table, df)
    #         df_list.append(df)

    # if df_list:

    #     # combine dataframes into one
    #     if len(df_list) > 1:
    #         combined_df = pd.concat(df_list)
    #     else:
    #         combined_df = df_list[0]

    # # write to sqlite db
    # insert_data(database_path, table, combined_df)
