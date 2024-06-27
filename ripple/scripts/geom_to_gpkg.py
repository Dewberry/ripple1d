import logging
import os
import shutil
import tempfile

import boto3
import pandas as pd
from pyproj import CRS

from ripple.errors import (
    CouldNotIdentifyPrimaryPlanError,
    NotAPrjFile,
)
from ripple.ras import RasFlowText, RasGeomText, RasPlanText, RasProject
from ripple.utils import get_sessioned_s3_client, str_from_s3


def geom_flow_to_gpkg(rg: RasGeomText, flow, plan_title: str, project_title: str, gpkg_file: str):
    if rg.cross_sections:
        geom_flow_xs_gdf(rg, flow, plan_title, project_title).to_file(gpkg_file, driver="GPKG", layer="XS")
    if rg.reaches:
        rg.reach_gdf.to_file(gpkg_file, driver="GPKG", layer="River")
    if rg.junctions:
        rg.junction_gdf.to_file(gpkg_file, driver="GPKG", layer="Junction")


def geom_flow_xs_gdf(rg: RasGeomText, flow, plan_title: str, project_title: str):
    xs_gdf = rg.xs_gdf
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
    xs_gdf["geom_title"] = rg.title
    xs_gdf["version"] = rg.version
    xs_gdf["flow_title"] = flow.title
    xs_gdf["project_title"] = project_title
    return xs_gdf


def detemine_primary_plan(
    ras_project: str,
    client: boto3.session.Session.client,
    crs: CRS,
    ras_text_file_path: str,
    bucket: str,
):
    if len(ras_project.plans) == 1:
        plan_path = ras_project.plans[0]
        string = str_from_s3(plan_path, client, bucket)
        return RasPlanText.from_str(string, crs, plan_path)
    for plan_path in ras_project.plans:
        string = str_from_s3(plan_path, client, bucket)
        candidate_plans = []
        if not string.__contains__("Encroach Node"):
            candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
    if len(candidate_plans) > 1 or not candidate_plans:
        raise CouldNotIdentifyPrimaryPlanError(f"Could not identfiy a primary plan for {ras_text_file_path}")
    else:
        return candidate_plans[0]


def geom_to_gpkg_s3(ras_text_file_path: str, crs: CRS, output_gpkg_path: str, bucket: str):
    client = get_sessioned_s3_client()

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read ras project file get list of plans
    string = str_from_s3(ras_text_file_path, client, bucket)
    ras_project = RasProject.from_str(string, ras_text_file_path)

    # determine primary plan
    plan = detemine_primary_plan(ras_project, client, crs, ras_text_file_path, bucket)

    # read flow string and write geopackage
    string = str_from_s3(plan.plan_steady_file, client, bucket)
    rf = RasFlowText.from_str(string, " .f01")

    # read geom string and write geopackage
    string = str_from_s3(plan.plan_geom_file, client, bucket)
    rg = RasGeomText.from_str(string, crs, " .g01")

    geom_flow_to_gpkg(rg, rf, plan.title, ras_project.title, temp_path)

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
