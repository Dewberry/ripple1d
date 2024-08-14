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
from botocore.exceptions import ParamValidationError
from pyproj import CRS

from ripple1d.data_model import NwmReachModel, RippleSourceModel
from ripple1d.errors import CouldNotIdentifyPrimaryPlanError, NoFlowFileSpecifiedError
from ripple1d.ras import VALID_GEOMS, VALID_STEADY_FLOWS, RasFlowText, RasGeomText, RasManager, RasPlanText, RasProject
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
from ripple1d.utils.ripple_utils import get_path
from ripple1d.utils.s3_utils import (
    get_basic_object_metadata,
    init_s3_resources,
    list_keys,
    s3_key_public_url_converter,
    str_from_s3,
)


def geom_flow_to_gpkg(
    ras_project: RasProject, crs: CRS, gpkg_file: str, client: boto3.client = None, bucket: str = None
) -> None:
    """Write geometry and flow data to a geopackage."""
    layers = geom_flow_to_gdfs(ras_project, crs, client, bucket)
    for layer, gdf in layers.items():
        gdf.to_file(gpkg_file, driver="GPKG", layer=layer)


def find_a_valid_file(
    directory: str, valid_extensions: list[str], client: boto3.client = None, bucket: str = None
) -> str:
    """Find a file in the directory that contains a valid extension. Returns the first valid file found."""
    if client and bucket:
        paths = list_keys(client, bucket, directory)
    else:
        paths = glob.glob(directory)
    for path in paths:
        if Path(path).suffix in valid_extensions:
            return path


def geom_flow_to_gdfs(
    ras_project: RasProject, crs: CRS, client: boto3.client = None, bucket: str = None
) -> gpd.GeoDataFrame:
    """Write geometry and flow data to a geopackage."""
    if client and bucket:

        rp = detemine_primary_plan(ras_project, crs, ras_project._ras_text_file_path, client, bucket)
        # get steady flow file
        try:
            plan_steady_file = get_path(rp.plan_steady_file, client, bucket)
        except NoFlowFileSpecifiedError as e:
            logging.warning(e)
            plan_steady_file = find_a_valid_file(ras_project._ras_dir, VALID_STEADY_FLOWS, client, bucket)

        string = str_from_s3(plan_steady_file, client, bucket)
        rf = RasFlowText.from_str(string, " .f01")

        # get geometry file
        try:
            plan_geom_file = get_path(rp.plan_geom_file, client, bucket)
        except NoFlowFileSpecifiedError:
            logging.warning(e)
            plan_geom_file = find_a_valid_file(ras_project._ras_dir, VALID_GEOMS, client, bucket)

        string = str_from_s3(plan_geom_file, client, bucket)
        rg = RasGeomText.from_str(string, crs, " .g01")
    else:
        rp = detemine_primary_plan(ras_project, crs, ras_project._ras_text_file_path)

        plan_steady_file = get_path(rp.plan_steady_file)
        rf = RasFlowText(plan_steady_file)

        plan_geom_file = get_path(rp.plan_geom_file)
        rg = RasGeomText(plan_geom_file, crs)

    layers = {}
    if rg.cross_sections:
        xs_gdf = rg.xs_gdf
        if "u" in Path(plan_steady_file).suffix:
            xs_gdf["flow_tile"] = rf.title
        else:
            xs_gdf = geom_flow_xs_gdf(rg, rf, xs_gdf)
        xs_gdf["plan_title"] = rp.title
        xs_gdf["geom_title"] = rg.title
        if len(rg.version) >= 1:
            xs_gdf["version"] = rg.version[0]
        else:
            xs_gdf["version"] = None
        xs_gdf["units"] = ras_project.units
        xs_gdf["project_title"] = ras_project.title
        layers["XS"] = xs_gdf

    if rg.reaches:
        layers["River"] = rg.reach_gdf
    if rg.junctions:
        layers["Junction"] = rg.junction_gdf

    if rg.structures:
        layers["Structure"] = rg.structures_gdf
    return layers


def geom_flow_xs_gdf(rg: RasGeomText, rf: RasFlowText, xs_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Create a geodataframe with cross section geometry and flow data."""
    xs_gdf[["flows", "profile_names"]] = None, None

    fcls = pd.DataFrame(rf.flow_change_locations)
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

    xs_gdf["flow_title"] = rf.title
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
        if client:
            plan_path = get_path(ras_project.plans[0], client, bucket)
            string = str_from_s3(plan_path, client, bucket)
            return RasPlanText.from_str(string, crs, plan_path)
        else:
            plan_path = get_path(ras_project.plans[0])
            return RasPlanText(plan_path, crs)
    candidate_plans = []
    for plan_path in ras_project.plans:
        plan_path = get_path(ras_project.plans[0], client, bucket)
        if client:
            try:
                string = str_from_s3(plan_path, client, bucket)
            except ParamValidationError as e:
                logging.warning(f"Missing plan file {plan_path} in {ras_project._ras_text_file_path}")
                continue
            if not string.__contains__("Encroach Node"):
                candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
        else:
            if os.path.exists(plan_path):
                with open(plan_path) as src:
                    string = src.read()
                if not string.__contains__("Encroach Node"):
                    candidate_plans.append(RasPlanText.from_str(string, crs, plan_path))
    if len(candidate_plans) > 1 or not candidate_plans:
        plan_path = ras_project._ras_root_path + "." + ras_project.current_plan.lstrip(".")
        if client:
            string = str_from_s3(plan_path, client, bucket)
            return RasPlanText.from_str(string, crs, plan_path)
        else:
            return RasPlanText(plan_path, crs)
        # raise CouldNotIdentifyPrimaryPlanError(f"Could not identfiy a primary plan for {ras_text_file_path}")
    else:
        return candidate_plans[0]


def geom_to_gpkg(ras_text_file_path: str, crs: CRS, output_gpkg_path: str):
    """Write geometry and flow data to a geopackage locally."""
    ras_project = RasProject(ras_text_file_path)
    geom_flow_to_gpkg(ras_project, crs, output_gpkg_path)


def geom_to_gpkg_s3(ras_text_file_path: str, crs: CRS, output_gpkg_path: str, bucket: str):
    """Write geometry and flow data to a geopackage on s3."""
    _, client, _ = init_s3_resources()

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read ras project file get list of plans
    string = str_from_s3(ras_text_file_path, client, bucket)
    ras_project = RasProject.from_str(string, ras_text_file_path)

    geom_flow_to_gpkg(ras_project, crs, temp_path, client, bucket)

    # move geopackage to s3
    output_gpkg_path = output_gpkg_path.replace(f"s3://{bucket}/", "")
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
    s3_ras_project_key: str,
    bucket: str,
    mip_case_no: str = None,
    dev_mode: bool = False,
):
    """Create a new stac item from a geopackage on s3."""
    logging.debug("Creating item from gpkg")
    # Instantitate S3 resources

    if mip_case_no is None:
        mip_case_no = "N/A"

    session, s3_client, s3_resource = init_s3_resources()
    item_basename = Path(s3_ras_project_key).name
    item_id = item_basename.replace(".prj", "")
    prefix = Path(s3_ras_project_key).parent.as_posix()
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
        "ripple: version": ripple1d.__version__,
        "ras version": data["version"],
        "project title": data["project_title"],
        "plan title": data["plan_title"],
        "geom title": data["geom_title"],
        "flow title": data["flow_title"],
        # "profile names": data["profile_names"].splitlines(),
        "MIP:case_ID": mip_case_no,
        "river miles": str(river_miles),
        "proj:wkt2": crs.to_wkt(),
        "proj:epsg": crs.to_epsg(),
    }

    item = create_geom_item(item_id, bbox, footprint, properties)
    rsm = RippleSourceModel(s3_ras_project_key, crs)

    asset_list = asset_list + [thumbnail_png_s3_key, gpkg_s3_key]
    for asset_key in asset_list:
        obj = s3_resource.Bucket(bucket).Object(asset_key)
        metadata = get_basic_object_metadata(obj)

        asset_info = get_asset_info(asset_key, rsm, bucket)
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


def new_stac_item(ras_project_directory: str, ras_s3_prefix: str):
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
        "ripple: version": ripple1d.__version__,
        "ras version": rm.version,
        "ras_units": rm.ras_project.units,
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

        asset_info = get_asset_info(asset_key, nwm_rm)
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
