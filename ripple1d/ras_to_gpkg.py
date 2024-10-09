"""Extract geospatial data from HEC-RAS files."""

import glob
import json
import logging
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path, PurePosixPath

import boto3
import geopandas as gpd
import pandas as pd
import pystac
from botocore.exceptions import ParamValidationError
from pyproj import CRS

import ripple1d
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
from ripple1d.utils.ripple_utils import fix_reversed_xs, get_path, prj_is_ras, xs_concave_hull
from ripple1d.utils.s3_utils import (
    get_basic_object_metadata,
    init_s3_resources,
    list_keys,
    s3_key_public_url_converter,
    str_from_s3,
)
from ripple1d.utils.sqlite_utils import create_non_spatial_table


def geom_flow_to_gpkg(
    ras_project: RasProject, crs: str, gpkg_file: str, metadata: dict, client: boto3.client = None, bucket: str = None
) -> None:
    """Write geometry and flow data to a geopackage."""
    layers, metadata = geom_flow_to_gdfs(ras_project, crs, metadata, client, bucket)
    for layer, gdf in layers.items():
        if layer == "XS":
            gdf = fix_reversed_xs(layers["XS"], layers["River"])
            if "Junction" in layers.keys():
                xs_concave_hull(gdf, layers["Junction"]).to_file(gpkg_file, driver="GPKG", layer="XS_concave_hull")
            else:
                xs_concave_hull(gdf).to_file(gpkg_file, driver="GPKG", layer="XS_concave_hull")
        gdf.to_file(gpkg_file, driver="GPKG", layer=layer)
    create_non_spatial_table(gpkg_file, metadata)
    return metadata


def find_a_valid_file(
    directory: str, valid_extensions: list[str], client: boto3.client = None, bucket: str = None
) -> str:
    """Find a file in the directory that contains a valid extension. Returns the first valid file found."""
    if client and bucket:
        paths = list_keys(client, bucket, directory)
    else:
        paths = glob.glob(f"{directory}/*")
    for path in paths:
        if Path(path).suffix in valid_extensions:
            return path


def gather_metdata(metadata: dict, ras_project: RasProject, rp: RasPlanText, rf: RasFlowText, rg: RasGeomText) -> dict:
    """Gather metadata from the ras project and its components."""
    metadata["plans_files"] = "\n".join(
        [get_path(i).replace(f"{ras_project._ras_dir}\\", "") for i in ras_project.plans if get_path(i)]
    )
    metadata["geom_files"] = "\n".join(
        [get_path(i).replace(f"{ras_project._ras_dir}\\", "") for i in ras_project.geoms if get_path(i)]
    )
    metadata["steady_flow_files"] = "\n".join(
        [get_path(i).replace(f"{ras_project._ras_dir}\\", "") for i in ras_project.steady_flows if get_path(i)]
    )
    metadata["unsteady_flow_files"] = "\n".join(
        [get_path(i).replace(f"{ras_project._ras_dir}\\", "") for i in ras_project.unsteady_flows if get_path(i)]
    )
    metadata["ras_project_file"] = ras_project._ras_text_file_path.replace(f"{ras_project._ras_dir}\\", "")
    metadata["ras_project_title"] = ras_project.title
    metadata["plans_titles"] = "\n".join(
        [RasPlanText(get_path(i), rg.crs).title for i in ras_project.plans if get_path(i)]
    )
    metadata["geom_titles"] = "\n".join(
        [RasGeomText(get_path(i), rg.crs).title for i in ras_project.geoms if get_path(i)]
    )
    metadata["steady_flow_titles"] = "\n".join(
        [RasFlowText(get_path(i)).title for i in ras_project.steady_flows if get_path(i)]
    )
    metadata["active_plan"] = get_path(ras_project._ras_root_path + ras_project.current_plan).replace(
        f"{ras_project._ras_dir}\\", ""
    )
    metadata["primary_plan_file"] = get_path(rp._ras_text_file_path).replace(f"{ras_project._ras_dir}\\", "")
    metadata["primary_plan_title"] = rp.title
    metadata["primary_flow_file"] = get_path(rf._ras_text_file_path).replace(f"{ras_project._ras_dir}\\", "")
    metadata["primary_geom_file"] = get_path(rg._ras_text_file_path).replace(f"{ras_project._ras_dir}\\", "")
    metadata["primary_geom_title"] = rg.title
    metadata["primary_flow_title"] = rf.title
    if len(rg.version) >= 1:
        metadata["ras_version"] = rg.version[0]
    else:
        metadata["ras_version"] = None
    metadata["ripple1d_version"] = ripple1d.__version__
    fcls = pd.DataFrame(rf.flow_change_locations)
    metadata["profile_names"] = "\n".join(fcls["profile_names"].iloc[0])
    metadata["units"] = ras_project.units
    return metadata


def geom_flow_to_gdfs(
    ras_project: RasProject, crs: str, metadata: dict, client: boto3.client = None, bucket: str = None
) -> tuple:
    """Write geometry and flow data to a geopackage."""
    if client and bucket:
        rp = detemine_primary_plan(ras_project, crs, ras_project._ras_text_file_path, client, bucket)
        # get steady flow file
        try:
            plan_steady_file = get_path(rp.plan_steady_file, client, bucket)
        except NoFlowFileSpecifiedError as e:
            logging.warning(e)
            plan_steady_file = find_a_valid_file(ras_project._ras_dir, VALID_STEADY_FLOWS, client, bucket)

        # get geometry file
        try:
            plan_geom_file = get_path(rp.plan_geom_file, client, bucket)
        except NoFlowFileSpecifiedError:
            logging.warning(e)
            plan_geom_file = find_a_valid_file(ras_project._ras_dir, VALID_GEOMS, client, bucket)

        string = str_from_s3(plan_steady_file, client, bucket)
        rf = RasFlowText.from_str(string, " .f01")

        string = str_from_s3(plan_geom_file, client, bucket)
        rg = RasGeomText.from_str(string, crs, " .g01")

    else:
        rp = detemine_primary_plan(ras_project, crs, ras_project._ras_text_file_path)

        try:
            plan_steady_file = get_path(rp.plan_steady_file)
        except NoFlowFileSpecifiedError as e:
            logging.warning(e)
            plan_steady_file = find_a_valid_file(ras_project._ras_dir, VALID_STEADY_FLOWS)

        try:
            plan_geom_file = get_path(rp.plan_geom_file)
        except NoFlowFileSpecifiedError as e:
            logging.warning(e)
            plan_geom_file = find_a_valid_file(ras_project._ras_dir, VALID_GEOMS)

        rf = RasFlowText(plan_steady_file)
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

    return layers, gather_metdata(metadata, ras_project, rp, rf, rg)


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
    ras_project: RasProject,
    crs: str,
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


def gpkg_from_ras(source_model_directory: str, crs: str, metadata: dict):
    """Write geometry and flow data to a geopackage locally."""
    logging.info("gpkg_from_ras starting")
    prjs = glob.glob(f"{source_model_directory}/*.prj")
    ras_text_file_path = None

    for prj in prjs:
        if prj_is_ras(prj):
            ras_text_file_path = prj
            break

    if not ras_text_file_path:
        raise FileNotFoundError(f"No ras project file found in {source_model_directory}")

    output_gpkg_path = ras_text_file_path.replace(".prj", ".gpkg")
    rp = RasProject(ras_text_file_path)
    logging.info("gpkg_from_ras complete")
    return geom_flow_to_gpkg(rp, crs, output_gpkg_path, metadata)


def gpkg_from_ras_s3(s3_prefix: str, crs: str, metadata: dict, bucket: str):
    """Write geometry and flow data to a geopackage on s3."""
    _, client, _ = init_s3_resources()

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    ras_text_file_path = None
    for key in list_keys(client, bucket, s3_prefix, ".prj"):
        string = str_from_s3(key, client, bucket)
        if "Proj Title" in string.split("\n")[0]:
            ras_text_file_path = key
            break

    if not ras_text_file_path:
        raise FileNotFoundError(f"No ras project file found in {s3_prefix}")

    # read ras project file get list of plans
    ras_project = RasProject.from_str(string, ras_text_file_path)

    geom_flow_to_gpkg(ras_project, crs, temp_path, metadata, client, bucket)

    # move geopackage to s3
    output_gpkg_path = ras_text_file_path.replace(f"s3://{bucket}/", "").replace(".prj", ".gpkg")
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
        if ras_s3_prefix:
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
