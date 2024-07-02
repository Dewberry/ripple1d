import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

import boto3
import geopandas as gpd
import pandas as pd
import pystac
from pyproj import CRS

from ripple.errors import CouldNotIdentifyPrimaryPlanError
from ripple.ras import RasFlowText, RasGeomText, RasPlanText, RasProject
from ripple.utils.dg_utils import bbox_to_polygon
from ripple.utils.gpkg_utils import (
    create_geom_item,
    create_thumbnail_from_gpkg,
    get_asset_info,
    get_river_miles,
    gpkg_to_geodataframe,
    reproject,
)
from ripple.utils.s3_utils import (
    get_basic_object_metadata,
    init_s3_resources,
    list_keys,
    s3_key_public_url_converter,
    str_from_s3,
)


def geom_flow_to_gpkg(rg: RasGeomText, flow, plan_title: str, project_title: str, gpkg_file: str):
    """Write geometry and flow data to a geopackage."""
    if rg.cross_sections:
        geom_flow_xs_gdf(rg, flow, plan_title, project_title).to_file(gpkg_file, driver="GPKG", layer="XS")
    if rg.reaches:
        rg.reach_gdf.to_file(gpkg_file, driver="GPKG", layer="River")
    if rg.junctions:
        rg.junction_gdf.to_file(gpkg_file, driver="GPKG", layer="Junction")


def geom_flow_xs_gdf(rg: RasGeomText, flow, plan_title: str, project_title: str) -> gpd.GeoDataFrame:
    """Create a geodataframe with cross section geometry and flow data."""
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
    crs: CRS,
    ras_text_file_path: str,
    client: boto3.session.Session.client = None,
    bucket: str = None,
) -> RasPlanText:
    """
    Determine the primary plan for a ras project.

    Exammple: The active plan if it does not contain encroachments.
    If the active plan contains encroachments, the first plan without encroachments is returned.
    If no plans are found without encroachments, an error is raised.
    """
    if len(ras_project.plans) == 1:
        plan_path = ras_project.plans[0]
        if client:
            string = str_from_s3(plan_path, client, bucket)
            return RasPlanText.from_str(string, crs, plan_path)
        else:
            return RasPlanText(plan_path, crs)
    candidate_plans = []
    for plan_path in ras_project.plans:

        if client:
            string = str_from_s3(plan_path, client, bucket)

            if not string.__contains__("Encroach Node"):
                candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
        else:
            if os.path.exists(plan_path):

                with open(plan_path) as src:
                    string = src.read()
                if not string.__contains__("Encroach Node"):
                    candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
    if len(candidate_plans) > 1 or not candidate_plans:
        raise CouldNotIdentifyPrimaryPlanError(f"Could not identfiy a primary plan for {ras_text_file_path}")
    else:
        return candidate_plans[0]


def geom_to_gpkg(ras_text_file_path: str, crs: CRS, output_gpkg_path: str):
    """Write geometry and flow data to a geopackage locally."""
    ras_project = RasProject(ras_text_file_path)

    # determine primary plan
    rp = detemine_primary_plan(ras_project, crs, ras_text_file_path)

    rf = RasFlowText(rp.plan_steady_file)

    rg = RasGeomText(rp.plan_geom_file, crs)

    geom_flow_to_gpkg(rg, rf, rp.title, ras_project.title, output_gpkg_path)


def geom_to_gpkg_s3(ras_text_file_path: str, crs: CRS, output_gpkg_path: str, bucket: str):
    """Write geometry and flow data to a geopackage on s3."""
    _, client, _ = init_s3_resources()

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read ras project file get list of plans
    string = str_from_s3(ras_text_file_path, client, bucket)
    ras_project = RasProject.from_str(string, ras_text_file_path)

    # determine primary plan
    rp = detemine_primary_plan(ras_project, client, crs, ras_text_file_path, bucket)

    # read flow string and write geopackage
    string = str_from_s3(rp.plan_steady_file, client, bucket)
    rf = RasFlowText.from_str(string, " .f01")

    # read geom string and write geopackage
    string = str_from_s3(rp.plan_geom_file, client, bucket)
    rg = RasGeomText.from_str(string, crs, " .g01")

    geom_flow_to_gpkg(rg, rf, rp.title, ras_project.title, temp_path)

    # move geopackage to s3
    logging.debug(f"uploading {output_gpkg_path} to s3")
    client.upload_file(
        Bucket=bucket,
        Key=output_gpkg_path,
        Filename=temp_path,
    )
    shutil.rmtree(temp_dir)


def new_stac_item_s3(
    gpkg_s3_key: str,
    new_stac_item_s3_key: str,
    thumbnail_png_s3_key: str,
    bucket: str,
    ripple_version: str,
    mip_case_no: str,
    dev_mode: bool = False,
):
    """Create a new stac item from a geopackage on s3."""
    logging.info("Creating item from gpkg")
    # Instantitate S3 resources

    session, s3_client, s3_resource = init_s3_resources(dev_mode)
    print(gpkg_s3_key.replace(Path(gpkg_s3_key).name, ""))
    asset_list = list_keys(s3_client, bucket, gpkg_s3_key.replace(Path(gpkg_s3_key).name, ""))

    gdfs = gpkg_to_geodataframe(f"s3://{bucket}/{gpkg_s3_key}")
    river_miles = get_river_miles(gdfs["River"])
    crs = gdfs["River"].crs
    gdfs = reproject(gdfs)

    logging.info("Creating png thumbnail")
    create_thumbnail_from_gpkg(gdfs, thumbnail_png_s3_key, bucket, s3_client)

    # Create item
    bbox = pd.concat(gdfs).total_bounds
    footprint = bbox_to_polygon(bbox)
    item = create_geom_item(
        gpkg_s3_key,
        bbox,
        footprint,
        ripple_version,
        gdfs["XS"].iloc[0],
        river_miles,
        crs,
        mip_case_no,
    )

    asset_list = asset_list + [thumbnail_png_s3_key, gpkg_s3_key]
    for asset_key in asset_list:
        obj = s3_resource.Bucket(bucket).Object(asset_key)
        metadata = get_basic_object_metadata(obj)
        asset_info = get_asset_info(asset_key, bucket)
        if asset_key == thumbnail_png_s3_key:
            asset = pystac.Asset(
                s3_key_public_url_converter(f"s3://{bucket}/{asset_key}"),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
        else:
            asset = pystac.Asset(
                f"s3://{bucket}/{asset_key}",
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
        item.add_asset(asset_info["title"], asset)

    s3_client.put_object(
        Body=json.dumps(item.to_dict()).encode(),
        Bucket=bucket,
        Key=new_stac_item_s3_key,
    )

    logging.info("Program completed successfully")
