import os
import re
from datetime import datetime
from pathlib import Path

import pystac

from ripple1d.ras import (
    RasFlowText,
    RasGeomText,
    RasPlanText,
)
from ripple1d.utils.s3_utils import get_basic_object_metadata, init_s3_resources, str_from_s3


def get_asset_string(asset_key: str, client, bucket: str = None) -> str:
    """Read an asset as a string."""
    # TODO: This function belongs in some util file
    if bucket:
        string = str_from_s3(asset_key, client, bucket)
    else:
        with open(asset_key) as in_file:
            string = in_file.read()
    return string


def get_asset_info(asset_key: str, bucket: str = None) -> dict:
    """Assign role, description, title, and extra fields to an asset."""
    base_asset = ras_plan_asset_info(asset_key)
    base_asset = add_extra_fields(asset_key, base_asset, bucket)
    return base_asset


def add_extra_fields(asset_key: str, base_asset: dict, bucket: str = None) -> dict:
    """Add extension-specific extra fields."""
    if bucket:
        _, client, s3_resource = init_s3_resources()
        obj = s3_resource.Bucket(bucket).Object(asset_key)
        base_asset["extra_fields"] = get_basic_object_metadata(obj)
    else:
        last_mod = os.path.getmtime(asset_key)
        last_mod = datetime.fromtimestamp(last_mod)
        last_mod = last_mod.isoformat()
        base_asset["extra_fields"] = {"file:size": os.path.getsize(asset_key), "last_modified": last_mod}
        client = None
    if Path(asset_key).suffix == ".hdf":
        return base_asset

    if "steady-flow-file" in base_asset["roles"]:
        asset_string = get_asset_string(asset_key, client, bucket)
        flow = RasFlowText.from_str(asset_string, " .f01")
        base_asset["extra_fields"]["Title"] = flow.title
        base_asset["extra_fields"]["Number of Profiles"] = flow.n_profiles
        base_asset["extra_fields"]["Profile Names"] = flow.profile_names
    if "geometry-file" in base_asset["roles"]:
        asset_string = get_asset_string(asset_key, client, bucket)
        geom = RasGeomText.from_str(asset_string, "EPSG:4326", " .g01")  # Dummy CRS
        base_asset["extra_fields"]["Title"] = geom.title
        base_asset["extra_fields"]["Number of rivers"] = geom.n_rivers
        base_asset["extra_fields"]["Number of reaches"] = geom.n_reaches
        base_asset["extra_fields"]["Number of cross sections"] = geom.n_cross_sections
        base_asset["extra_fields"]["Number of junctions"] = geom.n_junctions
    if "plan-file" in base_asset["roles"]:
        asset_string = get_asset_string(asset_key, client, bucket)
        plan = RasPlanText.from_str(asset_string, "EPSG:4326", " .p01")  # Dummy CRS
        base_asset["extra_fields"]["Title"] = plan.title

    return base_asset


def ras_plan_asset_info(s3_key: str) -> dict:
    """
    Generate information about a plan asset used in a HEC-RAS model.

    Parameters:
        s3_key (str): The S3 key of the asset.

    Returns:
        dict: A dictionary with the roles, the description, and the title of the asset.

    The function performs the following steps:
    1. Extracts the file extension and the file name from the provided `s3_key`.
    2. If the file extension is ".hdf", it sets the `ras_extension` to the extension of the file name without the
      ".hdf" suffix and adds `pystac.MediaType.HDF5` to the roles. Otherwise, it sets the `ras_extension` to the file
        extension.
    3. Removes the leading dot from the `ras_extension`.
    4. Depending on the `ras_extension`, it sets the roles and the description for the asset. The `ras_extension` is
      expected to match one of the HEC-RAS file types. If it doesn't match any of these patterns, it adds "ras-file" to the roles.
    5. Returns a dictionary with the roles, the description, and the title of the asset.
    """
    file_extension = Path(s3_key).suffix
    full_extension = s3_key.rsplit("/")[-1].split(".", 1)[1]
    title = Path(s3_key).name
    description = ""
    roles = []

    if file_extension == ".hdf":
        ras_extension = Path(s3_key.replace(".hdf", "")).suffix
        roles.append(pystac.MediaType.HDF5)
    else:
        ras_extension = file_extension

    ras_extension = ras_extension.lstrip(".")

    if re.match("[Gg][0-9]{2}", ras_extension):
        roles.extend(["geometry-file", "ras-file"])
        description = (
            """The geometry file which contains cross-sectional, hydraulic structures, and modeling approach data."""
        )
        if file_extension != ".hdf":
            roles.extend([pystac.MediaType.TEXT])

    elif re.match("[Pp][0-9]{2}", ras_extension):
        roles.extend(["plan-file", "ras-file"])
        description = """The plan file which contains a list of associated input files and all simulation options."""
        if file_extension != ".hdf":
            roles.extend([pystac.MediaType.TEXT])
    elif re.match("[Ff][0-9]{2}", ras_extension):
        roles.extend(["steady-flow-file", "ras-file", pystac.MediaType.TEXT])
        description = """Steady Flow file which contains profile information, flow data, and boundary conditions."""

    elif re.match("[Qq][0-9]{2}", ras_extension):
        roles.extend(["quasi-unsteady-flow-file", "ras-file", pystac.MediaType.TEXT])
        description = """Quasi-Unsteady Flow file."""

    elif re.match("[Uu][0-9]{2}", ras_extension):
        roles.extend(["unsteady-file", "ras-file", pystac.MediaType.TEXT])
        description = """The unsteady file contains hydrographs amd initial conditions, as well as any flow options."""

    elif re.match("[Rr][0-9]{2}", ras_extension):
        roles.extend(["run-file", "ras-file", pystac.MediaType.TEXT])
        description = """Run file for steady flow analysis which contains all the necessary input data required for the RAS computational engine."""

    elif re.match("hyd[0-9]{2}", ras_extension):
        roles.extend(["computational-level-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Detailed Computational Level output file."""

    elif re.match("[Cc][0-9]{2}", ras_extension):
        roles.extend(["geometric-preprocessor-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Geomatric Pre-Processor output file. Contains the hydraulic properties tables, rating curves, and family of rating curves for each cross-section, bridge, culvert, storage area, inline and lateral structure."""

    elif re.match("[Bb][0-9]{2}", ras_extension):
        roles.extend(["boundary-condition-file", "ras-file", pystac.MediaType.TEXT])
        description = """Boundary Condition file."""

    elif re.match("bco[0-9]{2}", ras_extension):
        roles.extend(["unsteady-flow-log-file", "ras-file", pystac.MediaType.TEXT])
        description = """Unsteady Flow Log output file."""

    elif re.match("[Ss][0-9]{2}", ras_extension):
        roles.extend(["sediment-data-file", "ras-file", pystac.MediaType.TEXT])
        description = """Sediment data file which contains flow data, boundary conditions, and sediment data."""

    elif re.match("[Hh][0-9]{2}", ras_extension):
        roles.extend(["hydraulic-design-file", "ras-file", pystac.MediaType.TEXT])
        description = """Hydraulic Design data file."""

    elif re.match("[Ww][0-9]{2}", ras_extension):
        roles.extend(["water-quality-file", "ras-file", pystac.MediaType.TEXT])
        description = """Water Quality data file which contains temperature boundary conditions, initial conditions, advection dispersion parameters and meteorological data."""

    elif re.match("SedCap[0-9]{2}", ras_extension):
        roles.extend(["sediment-transport-capacity-file", "ras-file", pystac.MediaType.TEXT])
        description = """Sediment Transport Capacity data."""

    elif re.match("SedXS[0-9]{2}", ras_extension):
        roles.extend(["xs-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Cross section output file."""

    elif re.match("SedHeadXS[0-9]{2}", ras_extension):
        roles.extend(["xs-output-header-file", "ras-file", pystac.MediaType.TEXT])
        description = """Header file for the cross section output."""

    elif re.match("wqrst[0-9]{2}", ras_extension):
        roles.extend(["water-quality-restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """The water quality restart file."""

    elif ras_extension == "sed":
        roles.extend(["sediment-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Detailed sediment output file."""

    elif ras_extension == "blf":
        roles.extend(["binary-log-file", "ras-file", pystac.MediaType.TEXT])
        description = """Binary Log file."""

    elif ras_extension == "prj" and title != "MMC_Projection.prj":
        roles.extend(["project-file", "ras-file", pystac.MediaType.TEXT])
        description = """Project file for ras. Contains current plan files, units, and project description."""

    elif ras_extension == "prj" and title == "MMC_Projection.prj":
        roles.extend(["projection-file", "ras-file", pystac.MediaType.TEXT])
        description = """Projection file."""

    elif ras_extension == "dss":
        roles.extend(["ras-dss", "ras-file"])
        description = """The dss file contains the dss results and other simulation information."""

    elif ras_extension == "log":
        roles.extend(["ras-log", "ras-file", pystac.MediaType.TEXT])
        description = """The log file contains the log information and other simulation information."""

    elif ras_extension == "png":
        roles.extend(["thumbnail", pystac.MediaType.PNG])
        description = """PNG of geometry with OpenStreetMap basemap."""
        title = "Thumbnail"

    elif ras_extension == "gpkg":
        roles.extend(["ras-geometry-gpkg", pystac.MediaType.GEOPACKAGE])
        description = """GeoPackage file with geometry data extracted from .gxx file."""
        title = "GeoPackage_file"

    elif ras_extension == "rst":
        roles.extend(["restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """Restart file."""
        title = "Restart_file"

    elif ras_extension == "SiamInput":
        roles.extend(["siam-input-file", "ras-file", pystac.MediaType.TEXT])
        description = """SIAM Input Data file."""

    elif ras_extension == "SiamOutput":
        roles.extend(["siam-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """SIAM Output Data file."""

    elif re.match("bco[0-9]{2}", ras_extension):
        roles.extend(["water-quality-log", "ras-file", pystac.MediaType.TEXT])
        description = """Water quality log file."""
        title = "Water_quality_log_file"

    elif ras_extension == "color_scales":
        roles.extend(["color-scales", "ras-file", pystac.MediaType.TEXT])
        description = """File that contains the water quality color scale."""

    elif full_extension == "comp_msgs.txt":
        roles.extend(["computational-message-file", "ras-file", pystac.MediaType.TEXT])
        description = """Computational Message text file which contains the computational messages that pop up in the computation window."""
        title = "Computational_message_file"

    elif re.match("[Xx][0-9]{2}", ras_extension):
        roles.extend(["run-file", "ras-file", pystac.MediaType.TEXT])
        description = """Run file for Unsteady Flow."""

    elif re.match("[Oo][0-9]{2}", full_extension):
        roles.extend(["output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Output file for ras which contains all of the computed results."""

    elif re.match("IC.O[0-9]{2}", full_extension):
        roles.extend(["initial-conditions-file", "ras-file", pystac.MediaType.TEXT])
        description = """Initial conditions file for unsteady flow plan."""

    elif re.match("[Pp][0-9]{2}.rst", full_extension):
        roles.extend(["restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """Restart file."""

    elif full_extension == "rasmap":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Ras Mapper file."""

    elif full_extension == "rasmap.backup":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Backup Ras Mapper file."""

    elif full_extension == "rasmap.original":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Original Ras Mapper file."""

    elif full_extension == "txt":
        roles.extend([pystac.MediaType.TEXT])
        description = """Miscellaneous text file."""

    elif full_extension == "xml":
        roles.extend([pystac.MediaType.XML])
        description = """Miscellaneous xml file."""

    return {"roles": roles, "description": description, "title": title}
