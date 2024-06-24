import logging
import os
import shutil
import sqlite3
import tempfile
import traceback

import boto3
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pyproj import CRS

from ripple.errors import (
    CouldNotIdentifyPrimaryPlanError,
    NotAPrjFile,
)
from ripple.ras import RasFlowText, RasGeomText, RasPlanText, RasProject
from ripple.ripple_logger import configure_logging


def geom_flow_to_gpkg(geom, flow, plan_title: str, project_title: str, gpkg_file: str):
    if geom.cross_sections:
        geom_flow_xs_gdf(geom, flow, plan_title, project_title).to_file(gpkg_file, driver="GPKG", layer="XS")
    if geom.reaches:
        geom.reach_gdf.to_file(gpkg_file, driver="GPKG", layer="River")
    if geom.junctions:
        geom.junction_gdf.to_file(gpkg_file, driver="GPKG", layer="Junction")


def geom_flow_xs_gdf(geom, flow, plan_title: str, project_title: str):
    xs_gdf = geom.xs_gdf
    xs_gdf[["flows", "profile_names"]] = None, None

    fcls = pd.DataFrame(flow.flow_change_locations)
    fcls["river_reach"] = fcls["river"] + fcls["reach"]

    for river_reach in fcls["river_reach"].unique():
        # get flow change locations for this reach
        fcls_rr = fcls.loc[fcls["river_reach"] == river_reach, :].sort_values(by="rs", ascending=False)

        # iterate through this reaches flow change locations and set cross section flows/profile names
        for _, row in fcls_rr.iterrows():

            # add flows to xs_gdf
            xs_gdf.loc[
                (xs_gdf["river"] == row["river"])
                & (xs_gdf["reach"] == row["reach"])
                & (xs_gdf["river_station"] <= row["rs"]),
                "flows",
            ] = "\n".join([str(f) for f in row["flows"]])

            # add profile names to xs_gdf
            xs_gdf.loc[
                (xs_gdf["river"] == row["river"])
                & (xs_gdf["reach"] == row["reach"])
                & (xs_gdf["river_station"] <= row["rs"]),
                "profile_names",
            ] = "\n".join(row["profile_names"])
    xs_gdf["plan_title"] = plan_title
    xs_gdf["geom_title"] = geom.title
    xs_gdf["version"] = geom.version
    xs_gdf["flow_title"] = flow.title
    xs_gdf["project_title"] = project_title
    return xs_gdf


def str_from_s3(ras_text_file_path, client):
    logging.debug(f"reading: {ras_text_file_path}")
    response = client.get_object(Bucket=bucket, Key=ras_text_file_path)
    return response["Body"].read().decode("utf-8")


def detemine_primary_plan(ras_project, client, crs, ras_text_file_path):
    if len(ras_project.plans) == 1:
        plan_path = ras_project.plans[0]
        string = str_from_s3(plan_path, client)
        return RasPlanText.from_str(string, crs, plan_path)
    for plan_path in ras_project.plans:
        string = str_from_s3(plan_path, client)
        candidate_plans = []
        if not string.__contains__("Encroach Node"):
            candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
    if len(candidate_plans) > 1 or not candidate_plans:
        raise CouldNotIdentifyPrimaryPlanError(f"Could not identfiy a primary plan for {ras_text_file_path}")
    else:
        return candidate_plans[0]


def geom_to_gpkg_s3(ras_text_file_path: str, crs: CRS, output_gpkg_path: str, bucket: str):
    # load s3 credentials
    load_dotenv(find_dotenv())

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    client = session.client("s3")

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read ras project file get list of plans
    string = str_from_s3(ras_text_file_path, client)
    ras_project = RasProject.from_str(string, ras_text_file_path)

    # determine primary plan
    plan = detemine_primary_plan(ras_project, client, crs, ras_text_file_path)

    # read flow string and write geopackage
    string = str_from_s3(plan.plan_steady_file, client)
    flow = RasFlowText.from_str(string, " .f01")

    # read geom string and write geopackage
    string = str_from_s3(plan.plan_geom_file, client)
    geom = RasGeomText.from_str(string, crs, " .g01")

    geom_flow_to_gpkg(geom, flow, plan.title, ras_project.title, temp_path)

    # move geopackage to s3
    logging.debug(f"uploading {output_gpkg_path} to s3")
    client.upload_file(
        Bucket=bucket,
        Key=output_gpkg_path,
        Filename=temp_path,
    )
    shutil.rmtree(temp_dir)


def process_one_geom(
    key: str,
    crs: str,
    bucket: str = None,
):
    # create path name for gpkg
    if key.endswith(".prj"):
        gpkg_path = key.replace("prj", "gpkg")
    else:
        raise NotAPrjFile(f"{key} does not have a '.prj' extension")

    # read the geometry and write the geopackage
    if bucket:
        geom_to_gpkg_s3(key, crs, gpkg_path, bucket)
    return f"s3://{bucket}/{gpkg_path}"


def read_case_db(cases_db_path: str, table_name: str):
    with sqlite3.connect(cases_db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT key, crs FROM inferred_crs_A ")
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


def main(cases_db_path: str, table_name: str, bucket: str = None):
    configure_logging(level=logging.ERROR, logfile="geom_to_gpkg.log")
    create_table(cases_db_path, "geom_gpkg_A")
    data = {}

    for key, crs in read_case_db(cases_db_path, table_name):
        key = key.replace("s3://fim/", "")
        gpkg_path, exc, tb = ["null"] * 3

        if key == "mip/dev2/Caney Creek-Lake Creek/BUMS CREEK/BUMS CREEK.prj":
            logging.info(f"working on {key}")
            try:
                gpkg_path = process_one_geom(key, crs, bucket)

            except Exception as e:
                exc = e
                tb = traceback.format_exc()
                logging.Error(exc)
            data[key] = {"crs": crs, "gpkg": gpkg_path, "exc": str(exc), "tb": tb}

    insert_data(cases_db_path, table_name, data)


if __name__ == "__main__":

    cases_db_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\cases.db"
    table_name = "texas_ble"
    bucket = "fim"  # "fim"
    # main(cases_db_path, table_name, bucket)

    prescribed_crs = 2277
    key = "mip/dev2/Caney Creek-Lake Creek/BUMS CREEK/BUMS CREEK.prj"
    process_one_geom(key, prescribed_crs, bucket)
