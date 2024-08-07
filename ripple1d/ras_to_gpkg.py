"""Extract geospatial data from HEC-RAS files."""

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath

import boto3
import geopandas as gpd
import pandas as pd
import pystac
from pyproj import CRS
from ripple1d.data_model import NwmReachModel
from ripple1d.errors import CouldNotIdentifyPrimaryPlanError
from ripple1d.ras import RasFlowText, RasGeomText, RasManager, RasPlanText, RasProject
from ripple1d.utils.dg_utils import bbox_to_polygon
from ripple1d.utils.gpkg_utils import (
    create_geom_item,
    create_thumbnail_from_gpkg,
    get_asset_info,
    get_river_miles,
    gpkg_to_geodataframe,
    reproject,
    write_thumbnail_to_s3,
)
from ripple1d.utils.s3_utils import (
    get_basic_object_metadata,
    init_s3_resources,
    list_keys,
    s3_key_public_url_converter,
    str_from_s3,
)


def geom_flow_to_gpkg(ras_text_file_path: str, crs: CRS, gpkg_file: str) -> None:
    """Write geometry and flow data to a geopackage."""
    layers = geom_flow_to_gdfs(ras_text_file_path, crs)
    for layer, gdf in layers.items():
        gdf.to_file(gpkg_file, driver="GPKG", layer=layer)


def geom_flow_to_gdfs(ras_text_file_path: str, crs: CRS) -> gpd.GeoDataFrame:
    """Write geometry and flow data to a geopackage."""
    ras_project = RasProject(ras_text_file_path)

    # determine primary plan
    plan_file = ras_text_file_path.replace(".prj", ras_project.current_plan)
    rp = RasPlanText(plan_file, crs)

    rf = RasFlowText(rp.plan_steady_file)

    rg = RasGeomText(rp.plan_geom_file, crs)

    layers = {}
    if rg.cross_sections:
        layers["XS"] = geom_flow_xs_gdf(rg, rf, rp.title, ras_project.title)
    if rg.reaches:
        layers["River"] = rg.reach_gdf
    if rg.junctions:
        layers["Junction"] = rg.junction_gdf
    return layers


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
    geom_flow_to_gpkg(geom_flow_to_gpkg, crs, output_gpkg_path)


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
    rp = detemine_primary_plan(ras_project, crs, ras_text_file_path, client, bucket)

    # read flow string and write geopackage
    string = str_from_s3(rp.plan_steady_file, client, bucket)
    rf = RasFlowText.from_str(string, " .f01")

    # read geom string and write geopackage
    string = str_from_s3(rp.plan_geom_file, client, bucket)
    rg = RasGeomText.from_str(string, crs, " .g01")

    geom_flow_to_gpkg(ras_text_file_path, crs, temp_path)

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
    ripple1d_version: str,
    mip_case_no: str,
    dev_mode: bool = False,
):
    """Create a new stac item from a geopackage on s3."""
    logging.debug("Creating item from gpkg")
    # Instantitate S3 resources

    session, s3_client, s3_resource = init_s3_resources()
    item_basename = Path(gpkg_s3_key).name
    item_id = item_basename.replace(".gpkg", "")
    prefix = gpkg_s3_key.replace(item_basename, "")
    asset_list = list_keys(s3_client, bucket, prefix)

    gdfs = gpkg_to_geodataframe(f"s3://{bucket}/{gpkg_s3_key}")
    river_miles = get_river_miles(gdfs["River"])
    crs = gdfs["River"].crs
    gdfs = reproject(gdfs)

    logging.debug("Creating png thumbnail")
    fig = create_thumbnail_from_gpkg(gdfs)
    write_thumbnail_to_s3(fig, thumbnail_png_s3_key, bucket, s3_client)

    # Create item
    bbox = pd.concat(gdfs).total_bounds
    footprint = bbox_to_polygon(bbox)

    data = gdfs["XS"].iloc[0]
    properties = {
        "ripple1d: version": ripple1d_version,
        "ras version": data["version"],
        "project title": data["project_title"],
        "plan title": data["plan_title"],
        "geom title": data["geom_title"],
        "flow title": data["flow_title"],
        "profile names": data["profile_names"].splitlines(),
        "MIP:case_ID": mip_case_no,
        "river miles": str(river_miles),
        "proj:wkt2": crs.to_wkt(),
        "proj:epsg": crs.to_epsg(),
    }

    item = create_geom_item(item_id, bbox, footprint, properties)

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

    logging.debug("Program completed successfully")


def new_stac_item(ras_project_directory: str, ripple1d_version: str, ras_s3_prefix: str):
    """Create a new stac item from a geopackage locally ."""
    logging.debug("Creating item from gpkg")

    nwm_rm = NwmReachModel(ras_project_directory)
    rm = RasManager(nwm_rm.ras_project_file, crs=nwm_rm.crs)
    gdfs = gpkg_to_geodataframe(nwm_rm.ras_gpkg_file)

    river_miles = get_river_miles(gdfs["River"])
    crs = gdfs["River"].crs
    gdfs = reproject(gdfs)

    logging.debug("Creating png thumbnail")
    fig = create_thumbnail_from_gpkg(gdfs)
    fig.savefig(nwm_rm.thumbnail_png)

    # Create item
    bbox = pd.concat(gdfs).total_bounds
    footprint = bbox_to_polygon(bbox)

    data = gdfs["XS"].iloc[0]
    properties = {
        "ripple1d: version": ripple1d_version,
        "ras version": rm.version,
        "project title": rm.ras_project.title,
        "plan titles": {key: val.file_extension for key, val in rm.plans.items()},
        "geom titles": {key: val.file_extension for key, val in rm.geoms.items()},
        "flow titles": {key: val.file_extension for key, val in rm.flows.items()},
        "river miles": str(river_miles),
        "NWM to_id": nwm_rm.ripple1d_parameters["nwm_to_id"],
        "proj:wkt2": crs.to_wkt(),
        "proj:epsg": crs.to_epsg(),
    }
    item = create_geom_item(nwm_rm.model_name, bbox, footprint, properties)

    for asset_key in nwm_rm.assets:
        asset_info = get_asset_info(asset_key, nwm_rm.model_directory)
        asset_key = str(PurePosixPath(Path(asset_key.replace(nwm_rm.model_directory, ras_s3_prefix))))
        asset = pystac.Asset(
            os.path.relpath(asset_key),
            extra_fields=asset_info["extra_fields"],
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        item.add_asset(asset_info["title"], asset)
    item.add_derived_from(nwm_rm.ripple1d_parameters["source_model"])
    item.add_derived_from(nwm_rm.ripple1d_parameters["source_terrain"])
    item.add_derived_from(nwm_rm.ripple1d_parameters["source_nwm_reach"])
    with open(nwm_rm.model_stac_json_file, "w") as dst:
        dst.write(json.dumps(item.to_dict()))

    logging.debug("Program completed successfully")
