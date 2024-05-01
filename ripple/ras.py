import os
import glob
import win32com.client
import datetime
import numpy as np
import h5py
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, LineString, Point
from dataclasses import dataclass
import boto3
import time
import re
import typing
import logging
import subprocess
import rasterio
import rasterio.mask
import pystac
from pyproj import CRS
from consts import (
    HDFGEOMETRIES,
    PLOTTINGSTRUCTURES,
    PLOTTINGREFERENCE,
)
from utils import decode, get_terrain_exe_path
from errors import ProjectionNotFoundError, NoDefaultEPSGError


@dataclass
class FlowChangeLocation:

    river: str = None
    reach: str = None
    rs: float = None
    rs_str: str = None

    flows: list = None


@dataclass
class XS:

    river: str = None
    reach: str = None
    river_reach: str = None
    rs: float = None

    left_reach_length: float = None
    channel_reach_length: float = None
    right_reach_length: float = None
    rs_str: str = None
    river_reach_rs: str = None

    description: str = None
    number_of_coords: int = None
    number_of_station_elevation_points: int = None
    coords: list = None
    station_elevation: list = None
    thalweg: float = None
    max_depth: float = None
    mannings: list = None
    left_bank: float = None
    right_bank: float = None


class Ras:
    """Represents a single HEC-RAS project.

    Attributes:
    ----------
    stac_href : href for the stac item representation of the HEC-RAS model
    stac_item : stac item representing the HEC-RAS model
    client : s3 client if reading from s3 (default=None)
    bucket : s3 bucket if treading from s3 (default=None)
    ras_folder : directory/s3 location where the ras model will be placed
    projection_file : projection file (.prj) contiaing the coordinate system
    projection : text representation of the projection
    plans : plans associated with the HEC-RAS project
    geoms : geometries associated with the HEC-RAS project
    flows : flows associated with the HEC-RAS project
    plan : active plan for the RAS project; can be set to any plan object in plans
    geom : active geom for the RAS project; can be set to any geom object in geoms
    flow : active flow for the RAS project; can be set to any flow object in flows
    title : title of the HEC-RAS project
    ras_project_basename : basename of the HEC-RAS poject (not always the same as the title)
    content : the contents of the HEC-RAS project file
    version : HEC-RAS version
    terrain_exe : executable for to build the RAS Terrain

    """

    def __init__(
        self,
        path: str,
        stac_href: str,
        s3_client: boto3.client = None,
        s3_bucket: str = None,
        version: str = "631",
        default_epsg: int = None,
    ):
        """

        Args:
            path (str): directory/s3 location where the ras model will be placed
            stac_href (str): href for the stac item representation of the HEC-RAS model
            s3_client (boto3.client, optional): s3 client if reading from s3. Defaults to None.
            s3_bucket (str, optional): s3 bucket if treading from s3. Defaults to None.
            version (str, optional): HEC-RAS version. Defaults to "631".
            default_epsg (int, optional): EPSG to default to if a projection cannot be found. Defaults to None.
        """

        self.client = s3_client
        self.bucket = s3_bucket

        self.ras_folder = path
        self.stac_href = stac_href
        self.stac_item = pystac.Item.from_file(self.stac_href)
        self.download_model()

        self.projection_file = None
        self.projection = ""
        self.get_ras_project_file()

        self.plans = {}
        self.geoms = {}
        self.flows = {}
        self.plan = None
        self.geom = None
        self.flow = None

        try:
            self.get_ras_projection()

        except ProjectionNotFoundError as e:

            print(e)

            if default_epsg:

                print(f"Attempting to use specified default projection: EPSG:{default_epsg}")

                self.projection = default_epsg

                self.projection_file = os.path.join(self.ras_folder, "projection.prj")

                with open(self.projection_file, "w") as f:
                    f.write(CRS.from_epsg(self.projection).to_wkt("WKT1_ESRI"))
            else:

                raise NoDefaultEPSGError(f"Could not identify projection from RAS Mapper and no default EPSG provided")

        self.read_content()

        self.title = self.content.splitlines()[0].split("=")[1]
        self.ras_project_basename = os.path.splitext(os.path.basename(self.ras_project_file))[0]
        self.get_plans()
        self.get_geoms()
        self.get_flows()
        self.get_active_plan()

        self.version = version
        self.terrain_exe = get_terrain_exe_path(self.version)

    def download_model(self):
        """
        Download HEC-RAS model form stac href
        """

        # make RAS directory if it does not exists
        if not os.path.exists(self.ras_folder):
            os.makedirs(self.ras_folder)

        # create stac item
        self.stac_item = pystac.Item.from_file(self.stac_href)

        # download HEC-RAS model files
        for name, asset in self.stac_item.assets.items():

            file = os.path.join(self.ras_folder, name)
            self.client.download_file(self.bucket, asset.href.replace("https://fim.s3.amazonaws.com/", ""), file)

    def read_content(self):
        """
        Attempt to read content of the RAS project text file. Checks both locally and on s3

        Raises:
            FileNotFoundError:
        """

        if os.path.exists(self.ras_project_file):

            with open(self.ras_project_file) as f:
                self.content = f.read()
        else:

            try:

                response = self.client.get_object(Bucket=self.bucket, Key=self.ras_project_file)
                self.content = response["Body"].read().decode()

            except Exception as E:

                print(E)

                raise FileNotFoundError(f"could not find {self.ras_project_file} locally nor on s3")

    def update_content(self):
        """
        Update the content of the RAS project with any changes made to the flow, geom, or plans.
        This does not update the actual file; use the 'write' method to do this.
        """

        lines = []
        for line in self.content.splitlines():

            if "Y Axis Title=Elevation" in line:

                for plan in self.plans.values():
                    lines.append(f"Plan File={plan.extension.lstrip('.')}")

                for geom in self.geoms.values():
                    lines.append(f"Geom File={geom.extension.lstrip('.')}")

                for flow in self.flows.values():
                    lines.append(f"Flow File={flow.extension.lstrip('.')}")

            if line.split("=")[0] not in ["Geom File", "Flow File", "Plan File"]:
                lines.append(line)

            self.content = "\n".join(lines)

    def set_current_plan(self, current_plan):
        """
        Update the current RAS plan in the RAS project content.
        This does not update the actual file; use the 'write' method to do this.

        Args:
            current_plan (Plan): The plan to set as the current plan
        """

        lines = []
        for line in self.content.splitlines():
            if "Current Plan=" in line:
                line = f"Current Plan={current_plan.extension.lstrip('.')}"
            lines.append(line)

        self.content = "\n".join(lines)

        self.plan = current_plan

    def write(self, ras_project_file=None):
        """
        Write the contents to the HEC-RAS project file

        Args:
            ras_project_file (_type_, optional): _description_. Defaults to None.
        """
        if not ras_project_file:
            ras_project_file = self.ras_project_file

        print(f"writing: {ras_project_file}")

        if os.path.exists(ras_project_file):

            with open(ras_project_file, "w") as f:
                f.write(self.content)

        else:

            self.client.put_object(Body=self.content, Bucket=self.bucket, Key=ras_project_file)

    def write_new_flow_rating_curves(self, title: str, reach_data: pd.DataFrame, normal_depth: float):
        """
        Write a new flow file contaning the specified title, reach_data, and normal depth.

        Args:
            title (str): Title of the flow file
            reach_data (pd.DataFrame) dataframe containing rows for each flow change location and columns for river,reach,us_rs,ds_rs, and flows.
            normal_depth (float): normal_depth to apply
        """

        # get a new extension number for the new flow file
        new_extension_number = self.get_new_extension_number(self.flows)
        text_file = self.ras_project_file.rstrip(".prj") + f".f{new_extension_number}"

        # create new flow
        flow = Flow(text_file)

        flow.content = ""

        flows = [int(i) for i in reach_data["flows_rc"]]

        # write headers
        flow.write_headers(title, flows)

        flow.profile_names = flows
        flow.profile_count = len(flow.profile_names)
        flow.title = title

        # write discharges
        flow.write_discharges(flows, self)

        # write normal depth
        flow.write_ds_normal_depth(reach_data, normal_depth, self)

        # write flow file content
        flow.write()

        # add new flow to the ras class
        self.flows[title] = flow

    def write_new_flow_production_runs(self, title: str, reach_data: pd.Series, normal_depth: float):
        """
        Write a new flow file contaning the specified title, reach_data, and normal depth.

        Args:
            title (str): Title of the flow file
            reach_data (pd.DataFrame) dataframe containing rows for each flow change location and columns for river,reach,us_rs,ds_rs, and flows.
            normal_depth (np.array): normal depth to apply at the downstream terminus of the reach.
        """

        # get a new extension number for the new flow file
        new_extension_number = self.get_new_extension_number(self.flows)
        text_file = self.ras_project_file.rstrip(".prj") + f".f{new_extension_number}"

        # create new flow
        flow = Flow(text_file)

        xs = self.geom.cross_sections

        # create profile names
        profile_names, flows, wses = flow.create_profile_names(reach_data, reach_data["flows"])

        flow.profile_names = profile_names
        flow.profile_count = len(flow.profile_names)
        flow.title = title

        flow.content = ""

        # write headers
        flow.write_headers(title, profile_names)

        # write discharges
        flow.write_discharges(flows, self)

        # write DS boundary conditions
        flow.write_ds_known_ws(reach_data, normal_depth, self)

        # add intermediate known wses
        flow.add_intermediate_known_wse(reach_data, wses)

        # write flow file content
        flow.write()

        # add new flow to the ras class
        self.flows[title] = flow

    def write_new_plan(self, geom, flow, title: str, short_id: str):
        """
        Write a new plan file with the given geom, flow, tite. and short ID.

        Args:
            geom (Geom): Geometry to set for this new plan.
            flow (Flow): Flow to set for this new plan.
            title (str): Title of this new plan.
            short_id (str): Short ID to set for this new plan.
        """
        # get a new extension number for the new plan
        new_extension_number = self.get_new_extension_number(self.plans)

        text_file = self.ras_project_file.rstrip(".prj") + f".p{new_extension_number}"

        # create plan
        plan = Plan(text_file, self.projection)

        # populate new plan info
        plan.new_plan(geom, flow, title, short_id)

        # write content
        plan.write()

        # add new plan to the ras class
        self.plans[title] = plan

    def get_new_extension_number(self, dict_of_ras_subclasses: dict) -> str:
        """
        Determines the next numeric extension that should be used when creating a new plan, flow, or geom;
        e.g., if you are adding a new plan and .p01, and .p02 already exists then the new plan will have a .p03 extension.

        Args:
            dict_of_ras_subclasses (dict): A dictionary containing plan/geom/flow titles as keys and objects plan/geom/flow as values.

        Returns:
            new file extension (str): The new file exension.
        """

        extension_number = []
        for val in dict_of_ras_subclasses.values():
            extension_number.append(int(val.extension[2:]))

        return f"{(max(extension_number)+1):02d}"

    def RunSIM(self, pid_running=None, close_ras=True, show_ras=False):
        """
        Run the current plan.

        Args:
            pid_running (_type_, optional): _description_. Defaults to None.
            close_ras (bool, optional): boolean to close RAS or not after computing. Defaults to True.
            show_ras (bool, optional): boolean to show RAS or not when computing. Defaults to True.
        """

        RC = win32com.client.Dispatch("RAS631.HECRASCONTROLLER")

        RC.Project_Open(self.ras_project_file)

        RC.Compute_CurrentPlan()
        while not RC.Compute_Complete():
            time.sleep(0.2)

        if show_ras:
            RC.ShowRas()

        if close_ras:
            RC.QuitRas()

    def clip_dem(self, src_path: str, dest_path: str = ""):
        """Clip the provided DEM raster to the concave hull of the cross sections

        Args:
            src_path (str): path to the source raster
            dest_path (str), optional): path to the dest raster. Defaults to a Terrain directory located in the RAS directory

        Returns:
            dest_path (str): path to the dest raster. Defaults to a Terrain directory located in the RAS directory
        """

        # if dest is not provided default to a Terrain directory located in the RAS directory
        if not dest_path:
            terrain_directory = os.path.join(self.ras_folder, "Terrain")

            if not os.path.exists(terrain_directory):
                os.makedirs(terrain_directory)

            dest_path = os.path.join(terrain_directory, "Terrain.tif")

        else:

            if not os.path.exists(os.path.dirname(dest_path)):
                os.makedirs(os.path.dirname(dest_path))

        # open the src raster the cross section concave hull as a mask
        with rasterio.open(src_path) as src:

            out_image, out_transform = rasterio.mask.mask(src, self.geom.xs_hull.to_crs(src.crs)["geometry"], crop=True)
            out_meta = src.meta

        # update metadata
        out_meta.update(
            {"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform}
        )

        # write dest raster
        with rasterio.open(dest_path, "w", **out_meta) as dest:
            dest.write(out_image)

        return dest_path

    def create_terrain(
        self,
        src_terrain_filepaths: typing.List[str],
        terrain_dirname: str = "Terrain",
        hdf_filename: str = "Terrain.hdf",
        vertical_units: str = "Feet",
    ):
        r"""
        Uses the projection file and a list of terrain file paths to make the RAS terrain HDF file.
        Default location is {model_directory}\Terrain\Terrain.hdf

        Parameters
        ----------
        src_terrain_filepaths : list[str]
            a list of terrain raster filepaths, typically tifs, to use when creating the terrain HDF
            can be a list of 1 filepath
        terrain_dirname : str (default="Terrain")
            the name of the directory to put the terrain HDF into
        hdf_filename : str (default="Terrain")
            the filename of the output HDF terrain file, with the extension
        vertical_units : str (default="Feet")
            vertical units to be used, must be one of ["Feet", "Meters"]
        """
        if vertical_units not in ["Feet", "Meters"]:
            raise ValueError(f"vertical_units must be either 'Feet' or 'Meters'; got: '{vertical_units}'")

        missing_files = [x for x in src_terrain_filepaths if not os.path.exists(x)]
        if missing_files:
            raise FileNotFoundError(str(missing_files))

        exe_parent_dir = os.path.split(self.terrain_exe)[0]
        terrain_dir_fp = os.path.join(self.ras_folder, terrain_dirname)
        subproc_args = [
            self.terrain_exe,
            "CreateTerrain",
            f"units={vertical_units}",  # vertical units
            "stitch=true",
            f"prj={self.projection_file}",
            f"out={os.path.join(terrain_dir_fp, hdf_filename)}",
        ]
        # add list of input rasters from which to build the Terrain
        subproc_args.extend([os.path.abspath(p) for p in src_terrain_filepaths])
        print(subproc_args)
        logging.info(f"Running the following args, from {exe_parent_dir}:" + "\n  ".join([""] + subproc_args))
        subprocess.check_call(subproc_args, cwd=exe_parent_dir, stdout=subprocess.DEVNULL)

        # TODO this recompression does work but RAS does not accept the recompressed tif for unknown reason...
        # # compress the output tif(s) that RasProcess.exe created (otherwise could be 1+ GB at HUC12 size)
        # layer_name = os.path.splitext(hdf_filename)[0]  # hdf file without extension
        # tif_pattern = rf"^{layer_name}\..+\.tif$"
        # output_tifs = [
        #     os.path.join(terrain_dir_fp, fn) for fn in os.listdir(terrain_dir_fp) if re.fullmatch(tif_pattern, fn)
        # ]
        # for tif in output_tifs:
        #     utils.recompress_tif(tif)
        #     utils.build_tif_overviews(tif)

    def get_active_plan(self):
        """
        Reads the content of the RAS project file to determine what the current plan is and sets the asociated plans,geoms,and flows as active.
        """

        if "Current Plan=" in self.content.splitlines()[1]:
            current_plan_extension = self.content.splitlines()[1].split("=")[1]

        for plan in self.plans.values():
            if plan.extension == f".{current_plan_extension}":
                self.plan = plan
                self.geom = plan.geom
                self.flow = plan.flow

    def get_plans(self):
        """
        Reads the contents of RAS project file to determine the plans that are associated with the RAS project
        then creates plan objects for each plan.
        """
        for plan_file in self.get_ras_files("Plan"):
            try:
                plan = Plan(plan_file, self.projection)
                self.plans[plan.title] = plan
            except FileExistsError as E:
                print(E)

    def get_geoms(self):
        """
        Reads the contents of RAS project file to determine the geoms that are associated with the RAS project
        then creates geom objects for each geom.
        """
        for geom_file in self.get_ras_files("Geom"):
            geom = Geom(geom_file, self.projection)
            self.geoms[geom.title] = geom

    def get_flows(self):
        """
        Reads the contents of RAS project file to determine the flows that are associated with the RAS project
        then creates flow objects for each flow.
        """
        for flow_file in self.get_ras_files("Flow"):
            flow = Flow(flow_file)
            self.flows[flow.title] = flow

    def get_ras_projection(self):
        """
        Attempts to find the RAS projection by reading the .rasmap file. Raises Errors if .rasmap or
        projection file can't be found or if the projection is not specified.

        Raises:
            FileNotFoundError: If .rasmap file can't be found.
            FileNotFoundError: If projection file can't be found.
            ValueError: If projection is not specified.
        """
        try:
            # look for .rasmap files
            try:

                rm = glob.glob(self.ras_folder + "/*.rasmap")[0]
            except IndexError as E:
                raise FileNotFoundError(f"Could not find a '.rasmap' file for this project.")

            # read .rasmap file to retrieve projection file.
            with open(rm) as f:
                lines = f.readlines()
                for line in lines:
                    if "RASProjectionFilename Filename=" in line:
                        relative_path = line.split("=")[-1].split('"')[1].lstrip(".")
                        self.projection_file = self.ras_folder + relative_path

            # check if the projection file exists
            if self.projection_file:
                if os.path.exists(self.projection_file):
                    if self.projection_file.endswith(".prj"):

                        with open(self.projection_file) as src:
                            self.projection = src.read()
                    else:
                        raise ValueError(f"Expected a projection file but got {self.projection_file}")
                else:
                    raise FileNotFoundError(f"Could not find projection file for this project")
            else:
                raise ValueError(f"No projection specified in .rasmap file.")

        except (FileNotFoundError, ValueError) as e:

            raise ProjectionNotFoundError(
                f"Could not determine the projection for this HEC-RAS model: {self.ras_folder}."
            )

    def get_ras_project_file(self):
        """
        Find the RAS project file given a directory making sure it is not simply a projection file.
        """
        prjs = glob.glob(self.ras_folder + "/*.prj")
        for prj in prjs:
            with open(prj) as f:
                lines = f.readlines()
                if "Proj Title=" in lines[0]:
                    self.ras_project_file = prj
                    break

    def get_ras_files(self, file_type: str) -> list:
        """
        Reads the RAS project file to determine what plan/flow/geom files are asociated with the RAS project.
        Only returns the associated file types specified by "file_type".

        Args:
            file_type (str): The file type to fiter results to; e.g., Flow,Geom,Plan.

        Returns:
            list: Returns a list of specified file types that are associated with the RAS project.
        """
        proj_title = os.path.basename(self.ras_project_file).split(".")[0]
        files = []
        for i in self.content.splitlines():
            if f"{file_type} File=" in i:
                file = i.strip("\n").split("=")[-1]
                files.append(self.ras_folder + f"/{proj_title}.{file}")

        return files


class BaseFile:
    """
    Represents a single ras file (plan,geom,flow).

    Attributes
    ----------
    text_file : an absolute filepath representing the model file
    hdf_file : hdf file for the geom, flow, or plan
    title : title of plan, geom, or flow
    content : a text representation of the files contents
    program_version : version of the HEC-RAS model
    client : s3 client if reading from s3 (default=None)
    bucket : s3 bucket if treading from s3 (default=None)
    extension : extension of the geom, flow, or plan; e.g., .g01, .p02, f05

    """

    def __init__(self, path: str, file_type: str, s3_client: boto3.client = None, s3_bucket: str = None):
        self.text_file = path
        self.hdf_file = path + ".hdf"
        self.extension = os.path.splitext(path)[1]
        self.content = None
        self.title = None

        self.bucket = s3_bucket
        self.client = s3_client

        if not os.path.exists(self.text_file):
            raise FileExistsError(f'The file "{self.hdf_file}" does not exists')

        self.read_content()

        self.title = self.content.splitlines()[0].split("=")[-1].rstrip("\n")
        self.program_version = self.content.splitlines()[1].split("=")[-1].rstrip("\n")

        if not os.path.exists(self.hdf_file):
            print(f'The file "{self.hdf_file}" does not exists')
            self.hdf_file = None

    def read_content(self):
        """
        Read content of the file. Searches for the file locally and on s3.

        Raises:
            FileNotFoundError:
        """
        if os.path.exists(self.text_file):
            with open(self.text_file) as f:
                self.content = f.read()
        else:
            try:
                response = self.client.get_object(Bucket=self.bucket, Key=self.text_file)
                self.content = response["Body"].read().decode()
            except Exception as E:
                print(E)
                raise FileNotFoundError(f"could not find {self.text_file} locally nor on s3")

    def write(self):
        """
        Write the content to file
        """

        print(f"writing: {self.text_file}")

        with open(self.text_file, "w") as src:
            src.write(self.content)


class Plan(BaseFile):
    """
    Represents a single plan file.

    Attributes
    ----------
    short_id : short ID for the plan
    projection : projection of the plan/RAS model
    geom : geom associated with the plan
    flow : flow associated with the plan
    date : simulation date
    dss_f : dss output file for the plan
    dss_interval : interval for the dss output
    """

    def __init__(self, path: str, projection: str = ""):
        """
        Args:
            path (str): file path to the plan text file
            projection (str, optional): projection file for the plan. Defaults to "".
        """
        try:
            super().__init__(path, "Plan")

        except FileExistsError as e:
            print(f"The plan file provided does not exists: {path}")

        self.short_id = None
        self.projection = projection

        self.parse_attrs()

    def read_rating_curves(self) -> dict:
        """
        Read the flow and water surface elevations resulting from the computed plan

        Raises:
            FileNotFoundError: _description_

        Returns:
            dict: A dictionary containing "wse" and "flow" keys whose values are pandas dataframes
        """

        # check if the hdf file exists; raise error if it does not
        if not self.hdf_file:
            self.hdf_file = self.text_file + ".hdf"

            if not os.path.exists(self.hdf_file):
                raise FileNotFoundError(f'The file "{self.hdf_file}" does not exists')

        def remove_multiple_spaces(x):
            return re.sub(" +", " ", x)

        # read the hdf file
        with h5py.File(self.hdf_file) as hdf:
            wse_path = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Water Surface"
            flow_path = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Cross Sections/Flow"
            columns_path = "/Results/Steady/Output/Geometry Info/Cross Section Only"
            index_path = "/Results/Steady/Output/Output Blocks/Base Output/Steady Profiles/Profile Names"

            # get columns and indexes for the wse and flow arrays
            columns = decode(pd.DataFrame(hdf[columns_path]))
            index = decode(pd.DataFrame(hdf[index_path]))

            # remove multiple spaces in between the river-reach-riverstation ids.
            columns[0] = columns[0].apply(remove_multiple_spaces)

            # create dataframes for the wse and flow results
            wse = pd.DataFrame(hdf.get(wse_path), columns=columns[0].values, index=index[0].values).T
            flow = pd.DataFrame(hdf.get(flow_path), columns=columns[0].values, index=index[0].values).T

        return {"wse": wse, "flow": flow}

    def parse_attrs(self):
        """
        Parse basic attributes from the plan text file.
        """
        for line in self.content.splitlines():
            if "Geom File=" in line:
                self.geom = Geom(
                    f'{os.path.splitext(self.text_file)[0]}.{line.split("=")[1]}',
                    self.projection,
                )
            elif "Flow File=" in line:
                self.flow = Flow(f'{os.path.splitext(self.text_file)[0]}.{line.split("=")[1]}')
            elif "Simulation Date=" in line:
                self.date = line.split("=")[1]
            elif "Short Identifier" in line:
                self.short_id = line.split("=")[1]
            elif "DSS File=" in line:
                self.dss_f = line.split("=")[1]
            elif "Output Interval" == line.split("=")[0]:
                self.dss_interval = line.split("=")[1]

    def convert_24_00_hour(self, date_time_str_array: np.array):
        """
        Convert RAS 24 hour notation to  00 hour

        Args:
            date_time_str_array (np.array): array containing ras date strings

        Returns:
            np.array: An array of of datetime objects
        """

        time_date = []
        for td in date_time_str_array:
            try:
                td = datetime.datetime.strptime(td, "%d%b%Y %H:%M:%S")
            except:
                td = td.replace(" 24:", " 23:")
                td = datetime.datetime.strptime(td, "%d%b%Y %H:%M:%S")
                td += datetime.timedelta(hours=1)
            time_date.append(td.strftime("%Y-%m-%d %H:%M:%S"))

        return np.array(time_date)

    def new_plan(self, geom, flow, title: str, short_id: str):
        """
        create a new plan with geom, flow, title, and short_id provided

        Args:
            geom (_type_): geom to associate with this new plan
            flow (_type_): flow to associate with this new plan
            title (str): title for this new plan
            short_id (str): short id for this new plan
        """

        # assign basic attributes for the new plan
        self.title = title
        self.short_id = short_id
        self.geom = geom
        self.flow = flow

        # populate the attributes above in the content of the plan
        self.populate_content()

    def populate_content(self):
        """
        populate the content of the plan with basic attributes (title, short_id, flow, and geom)

        Raises:
            RuntimeError: raise run time error if the plan already has content associated with it
        """

        if self.content:
            raise RuntimeError(f"content already exists for this plan: {self.text_file}")

        # create necessary lines for the content of the plan text file.
        lines = []
        lines.append(f"Plan Title={self.title}")
        lines.append(f"Short Identifier={self.short_id}")
        lines.append(f"Geom File={self.geom.extension.lstrip('.')}")
        lines.append(f"Flow File={self.flow.extension.lstrip('.')}")
        lines.append(f"Run RASMapper=-1 ")
        self.content = "\n".join(lines)


class Geom(BaseFile):
    """
    Represents a single geom file.

    Attributes
    ----------
    projection : projection of the plan/RAS model
    """

    def __init__(self, path: str, projection: str = ""):
        super().__init__(path, "Geom")
        self.projection = projection
        self.cross_sections = None

    def determine_wse_increments_for_xs(self, increment: float):
        """
        Determine elevation increments for each cross section using the increments starting at the
        cross sections thalweg and increaseing to the max elevation of the cross section. Populate the incremented
        elevations and depths along with the thalweg and elevation_count in the cross sections geodataframe.

        Args:
            increment (float): elevation increment to use

        """

        elevations, elevation_count, thalwegs, depths = [], [], [], []

        # iterate through the cross sections
        for _, row in self.cross_sections.iterrows():

            # create a dataframe from the station-elevation data for this xs
            se = pd.DataFrame(row["station_elevation"])

            # determine the thalweg of the xs
            thalweg = np.ceil(se["elevation"].min() * 2) / 2

            # determine the max elevation of the cross section
            max_elevation = se["elevation"].max()

            # compute the elevation increments for this cross section
            elevation = list(np.arange(thalweg, max_elevation + increment, increment))

            # convert elevaitons to depth using the thalweg
            depth = [i - thalweg for i in elevation]

            # append the incremented elevations and depths and the thalweg and elevation_count
            elevations.append(elevation)
            elevation_count.append(len(elevation))
            thalwegs.append(thalweg)
            depths.append(depth)

        # add relevant columns to the cross section geodataframe
        self.cross_sections["wses"] = elevations
        self.cross_sections["wse_count"] = elevation_count
        self.cross_sections["thalweg"] = thalwegs
        self.cross_sections["depths"] = depths

    def us_ds_most_xs(self, us_ds: str = "us") -> pd.DataFrame:
        """
        Determine the upstream or downstream most cross section for a RAS river-reach

        Args:
            us_ds (str, optional): specify "us", or "ds". Defaults to "us".

        Raises:
            ValueError: Raise a value error if something other that "us" or "ds" is provided

        Returns:
            pd.DataFrame: a pandas dataframe containing the upstream or downstream most cross sections
        """

        if not isinstance(self.cross_sections, gpd.GeoDataFrame):
            self.scan_for_xs()

        us_ds_most_xs = []

        # iterate through unique river-reach combos
        for i in self.cross_sections["river_reach"].unique():

            # get min or max river station for each unique river-reach combo
            if us_ds == "us":
                rs = self.cross_sections.loc[self.cross_sections["river_reach"] == i, "rs"].max()
            elif us_ds == "ds":
                rs = self.cross_sections.loc[self.cross_sections["river_reach"] == i, "rs"].min()
            else:
                raise ValueError(f"Expected 'us' or 'ds' for us_ds arg but recieved {us_ds}")

            # return the unique river and reach
            df = self.cross_sections.loc[(self.cross_sections["river_reach"] == i) & (self.cross_sections["rs"] == rs)]

            # compile river,reach, and river station for each ds most xs
            us_ds_most_xs.append(df)

        return pd.concat(us_ds_most_xs)

    def scan_for_xs(self) -> gpd.GeoDataFrame:
        """
        Scan the geom for cross sections

        Returns:
            gpd.GeoDataFrame: A geodataframe containing the cross sections in the geometry
        """

        lines = self.content.splitlines()

        cross_sections = []
        xs = None

        # iterate through the lines in the geom contents
        for i, line in enumerate(lines):

            # parse river and reach
            if "River Reach=" in line:
                river, reach = line.lstrip("River Reach=").split(",")

            # parse river station and reach lengths for the left, channel, and right flowpaths
            if "Type RM Length L Ch R =" in line:

                xs = self.parse_reach_lengths(line, river.rstrip(" "), reach.rstrip(" "))

            # if a cross section is identified
            if xs:

                if "XS GIS Cut Line=" in line:

                    # parse coordinates of the cross section
                    xs = self.parse_number_of_coords(line, xs)
                    xs = self.parse_coords(lines[i + 1 :], xs)

                if "#Sta/Elev=" in line:

                    # parse station-elevation data
                    xs = self.parse_number_of_station_eleveation_points(line, xs)
                    xs = self.parse_station_elevation_points(lines[i + 1 :], xs)

                if "Bank Sta=" in line:

                    # parse bank stations
                    xs = self.parse_bank_stations(line, xs)

                    # gather cross sections
                    cross_sections.append(xs)

                    xs = None

        if cross_sections:

            # create cross section geodataframe
            self.cross_sections = gpd.GeoDataFrame(cross_sections, geometry="coords", crs=self.projection)

            # create a concave hull for the cross sections
            self.xs_hull = self.xs_concave_hull(self.cross_sections)

    def parse_reach_lengths(self, line: str, river: str, reach: str):
        """
        Parse the reach lengths for a cross section from the geom content

        Args:
            line (str): lines from the geom content
            river (str): name of the river that this cross section is associated with
            reach (str): name of the reach that this cross section is associated with

        Returns:
            XS: a dataclass representing a cross section
        """

        # parse the Type, rs, left_reach_length, channel_reach_length, and right_reach_length
        Type, rs, left_reach_length, channel_reach_length, right_reach_length = line.lstrip(
            "Type RM Length L Ch R ="
        ).split(",")

        # type 1 indicates a cross section
        if Type == "1 ":

            # create handy river-reach-rs id
            river_reach_rs = f"{river} {reach} {rs.rstrip(' ')}"

            # create XS dataclass
            xs = XS(
                river,
                reach,
                f"{river}_{reach}",
                float(rs),
                float(left_reach_length),
                float(channel_reach_length),
                float(right_reach_length),
                rs.rstrip(" "),
                river_reach_rs,
            )

            return xs
        else:
            return None

    def parse_description(self, lines: list, xs: XS):
        # TODO
        pass

    def parse_number_of_coords(self, line: str, xs: XS):
        """
        parse the number of coordinates for a cross section

        Args:
            line (str): line of a goem content conntaining "XS GIS Cut Line="
            xs (XS): XS data class for this cross section

        Returns:
            _type_: XS data class for this cross section with number of coordinates populated
        """

        if "XS GIS Cut Line=" in line:
            xs.number_of_coords = int(line.lstrip("XS GIS Cut Line="))
            return xs

    def parse_coords(self, lines: list, xs: XS):
        """
        Parse the coordinates for this cross section from the geom content

        Args:
            lines (list): lines form the geom content
            xs (XS): XS data class for this cross section

        Returns:
            XS: XS dataclass for this cross section with coordinates populated
        """

        coords = []
        for line in lines:
            for i in range(0, len(line), 32):
                x = line[i : i + 16]
                y = line[i + 16 : i + 32]
                coords.append((float(x), float(y)))

                if len(coords) >= xs.number_of_coords:
                    xs.coords = LineString(coords)
                    return xs

    def parse_number_of_station_eleveation_points(self, line: str, xs: XS):
        """
        Parse the number of station elevation points from the line containing "#Sta/Elev="

        Args:
            line (str): line containing "#Sta/Elev=" from the geom content
            xs (XS): XS data class for this cross section

        Returns:
            _type_: XS data class for this cross section with number of station-elevation points populated
        """

        if "#Sta/Elev=" in line:

            xs.number_of_station_elevation_points = int(line.lstrip("#Sta/Elev="))

            return xs

    def parse_station_elevation_points(self, lines: list, xs: XS):
        """
        Parse the station elevation points from the geom content

        Args:
            lines (list): lines from the geom content
            xs (XS): XS dataclass for this cross section

        Returns:
            _type_: XS dataclass for this cross section with station-elevation populated
        """

        se = []

        for line in lines:

            for i in range(0, len(line), 16):

                s = line[i : i + 8]
                e = line[i + 8 : i + 16]
                se.append((float(s), float(e)))

                if len(se) >= xs.number_of_station_elevation_points:
                    df = pd.DataFrame(se, columns=["station", "elevation"])

                    # compute thalweg
                    xs.thalweg = df["elevation"].min()

                    # compute max depth for the cross section
                    xs.max_depth = min(df["elevation"].iloc[0], df["elevation"].iloc[-1]) - xs.thalweg

                    xs.station_elevation = df.to_dict()

                    return xs

    def parse_bank_stations(self, line: list, xs: XS):
        """
        Parse bank stations

        Args:
            line (list): line from the geom content containing "Bank Sta=".
            xs (XS): XS dataclass for this cross section

        Returns:
            _type_: XS dataclass for this cross section populated with bank stations
        """
        left_bank, right_bank = line.strip("Bank Sta=").split(",")
        xs.left_bank = left_bank
        xs.right_bank = right_bank

        return xs

    def xs_concave_hull(self, xs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Compute the concave hull for the cross sections in the geom

        Args:
            xs (gpd.GeoDataFrame): cross sections geodataframe

        Returns:
            gpd.GeoDataFrame: concave hull geodataframe
        """

        points = xs.boundary.explode().unstack()
        points_last_xs = [Point(coord) for coord in xs["coords"].iloc[-1].coords]
        points_first_xs = [Point(coord) for coord in xs["coords"].iloc[0].coords[::-1]]

        polygon = Polygon(points_first_xs + list(points[0]) + points_last_xs + list(points[1])[::-1])

        return gpd.GeoDataFrame({"geometry": [polygon]}, geometry="geometry", crs=xs.crs)


class Flow(BaseFile):
    """
    Represents a single flow file.

    Attributes
    ----------
    flow_change_locations : cross sections where flows are applied
    profile_count : number of flow profiles
    profile_names : names of the flow profiles
    max_flow_applied : the maximum flow applied

    """

    def __init__(self, path: str):
        """

        Args:
            path (str): path to the flow file
        """

        try:

            super().__init__(path, "Flow")
            self.flow_change_locations = []
            self.parse_attrs()

            if self.flow_change_locations:
                self.max_flow_applied()

        except FileExistsError as e:

            print(f"The flow file provided does not exists: {path}")

    def parse_attrs(self):
        """
        Parse the basic attributes from the contents of the flow

        """

        lines = self.content.splitlines()

        for i, line in enumerate(lines):

            if "Number of Profiles=" in line:
                self.profile_count = int(line.lstrip("Number of Profiles="))

            elif "Profile Names=" in line:
                self.profile_names = line.lstrip("Profile Names=").split(",")

            elif "River Rch & RM=" in line:
                flow = self.parse_flows(lines[i:])
                self.flow_change_locations.append(flow)

            elif "Boundary for River Rch & Prof#=" in line:
                pass

    def parse_flows(self, lines: list):
        """
        Parse the flows from the flow content

        Args:
            lines (list): lines from the flow content

        Returns:
            FlowChangeLocation: FlowChangeLocation object
        """

        # parse river, reach, and river station for the flow change location
        river, reach, rs = lines[0].lstrip("River Rch & RM=").split(",")

        flows = []
        for line in lines[1:]:

            for i in range(0, len(line), 8):

                if len(flows) == self.profile_count:

                    return FlowChangeLocation(river, reach.rstrip(" "), float(rs), rs, flows)

                flows.append(float(line[i : i + 8].lstrip(" ")))

    def max_flow_applied(self):
        """
        Compute max flow applied
        """

        self.max_flow_applied = max([max(flow.flows) for flow in self.flow_change_locations])

    def create_profile_names(self, reach_data: pd.Series, input_flows: np.array) -> tuple:
        """
        Create profile names from flows and ds_depths specified in the reach_data

        Args:
            reach_data (pd.Series): NWM reach data
            input_flows (np.array): Flows to create profiles names from. Combine with incremental depths
            of the downstream cross section of the reach

        Returns:
            tuple: tuple of profile_names, flows, and wses
        """

        profile_names, flows, wses = [], [], []

        for e, depth in enumerate(reach_data["ds_depths"]):

            for flow in input_flows:

                profile_names.append(f"{flow}_{depth}")

                flows.append(flow)
                wses.append(reach_data["ds_wses"][e])

        return (profile_names, flows, wses)

    def write_headers(self, title: str, profile_names: list):
        """
        Write headers for flow content

        Args:
            title (str): title of the flow
            profile_names (list): profile names for the flow

        Returns:
            list: lines of the flow content
        """
        lines = []
        lines.append(f"Flow Title={title}")
        lines.append(f"Number of Profiles= {len(profile_names)}")
        lines.append(f"Profile Names={','.join([str(pn) for pn in profile_names])}")

        self.content += "\n".join(lines)

    def write_discharges(self, flows: list, r: Ras):
        """
        Write discharges to flow content

        Args:
            flows (list): flows to write to the flow content
            r (Ras ): Ras object representing the HEC-RAS project
        """

        us_cross_sections = r.geom.us_ds_most_xs(us_ds="us")

        lines = []
        line = ""

        for _, xs in us_cross_sections.iterrows():

            lines.append(f"River Rch & RM={xs.river},{xs.reach.ljust(16,' ')},{str(xs.rs).rstrip("0").rstrip(".").ljust(8,' ')}")

            for i, flow in enumerate(flows):

                line += f"{str(int(flow)).rjust(8,' ')}"

                if (i + 1) % 10 == 0:

                    lines.append(line)
                    line = ""

            if (i + 1) % 10 != 0:
                lines.append(line)

        self.content += "\n" + "\n".join(lines)

    def write_ds_known_ws(self, reach_data: pd.Series, normal_depth: float, r: Ras):
        """
        Write downstream known water surface elevations to flow content

        Args:
            reach_data (pd.Series): Reach data from NWM reaches
            normal_depth (float): Normal depth to apply if reach_data does not contain a downstream river station (ds_rs)
            r (Ras): Ras object representing the HEC-RAS project
        """

        ds_cross_sections = r.geom.us_ds_most_xs(us_ds="ds")
        count = 0
        

        for _, xs in ds_cross_sections.iterrows():

            if reach_data["ds_rs"] == xs["rs"]:

                # get the downstream wses for this reach/xs
                wses = reach_data["ds_wses"]
                lines = []
                for e, wse in enumerate(wses):
                    
                    for i in range(len(reach_data["flows"])):
                        count += 1
                        lines.append(f"Boundary for River Rch & Prof#={xs.river},{xs.reach.ljust(16,' ')}, {count}")
                        lines.append(f"Up Type= 0 ")
                        lines.append(f"Dn Type= 1 ")
                        lines.append(f"Dn Known WS={wse}")

                    self.content += "\n" + "\n".join(lines)
            else:
                self.write_ds_normal_depth(reach_data, normal_depth, r)

        

    def write_ds_normal_depth(self, reach_data: pd.Series, normal_depth: float, r: Ras):
        """
        Write the downstream normal depth boundary condition

        Args:
            reach_data (pd.Series): Reach data from NWM reaches
            normal_depth (float): Normal depth slope to apply to all profiles
            r (Ras): Ras object representing the HEC-RAS project
        """
        ds_cross_sections = r.geom.us_ds_most_xs(us_ds="ds")
        count = 0
        lines = []

        for _, xs in ds_cross_sections.iterrows():

            for i in range(len(reach_data["flows_rc"])):
                count += 1
                lines.append(f"Boundary for River Rch & Prof#={xs.river},{xs.reach.ljust(16,' ')}, {count}")
                lines.append(f"Up Type= 0 ")
                lines.append(f"Dn Type= 3 ")
                lines.append(f"Dn Slope={normal_depth}")

        self.content += "\n" + "\n".join(lines)

    def add_intermediate_known_wse(self, reach_data: pd.Series, wses: list):
        """
        Write known water surface elevations for intermediate cross sections along the reach to the flow content.

        Args:
            reach_data (pd.Series): Reach data from NWM reaches
            wses (list): known water surface elvations to apply
        """

        lines = []

        reach_data["ds_rs"]
        for e, wse in enumerate(wses):

            lines.append(
                f"Set Internal Change={reach_data['river']}       ,{reach_data['reach']}         ,{reach_data['ds_rs']}  , {e+1} , 3 ,{wse}"
            )

        self.content += "\n" + "\n".join(lines)


class RasMap:
    """
    Represents a single RasMapper file.

    Attributes
    ----------
    text_file : Text file repressenting the RAS Mapper file
    content : a text representation of the files contents
    version : HEC-RAS version for this RAS Mapper file
    client : s3 client if reading from s3 (default=None)
    bucket : s3 bucket if treading from s3 (default=None)

    """

    def __init__(self, path: str, version: str = 631, s3_bucket=None, s3_client=None):
        """

        Args:
            path (str): file path to the plan text file
            version (str): HEC-RAS vesion for this RAS Mapper file
            s3_bucket (_type_, optional): s3 client if reading from s3. Defaults to None.
            s3_client (_type_, optional): s3 bucket if reading from s3. Defaults to None.
        """

        self.text_file = path
        self.content = ""
        self.version = version

        self.bucket = s3_bucket
        self.client = s3_client

        if os.path.exists(path):
            self.read_content()

        else:
            self.new_rasmap_content()

    def read_content(self):
        """
        Read content of the file. Searches for the file locally and on s3.

        Raises:
            FileNotFoundError:
        """
        if os.path.exists(self.text_file):
            with open(self.text_file) as f:
                self.content = f.read()
        else:
            try:
                response = self.client.get_object(Bucket=self.bucket, Key=self.text_file)
                self.content = response["Body"].read().decode()
            except Exception as E:
                print(E)
                raise FileNotFoundError(f"could not find {self.text_file} locally nor on s3")

    def new_rasmap_content(self):
        """
        Populate the content with boiler plate content
        """
        if self.version in ["631", "6.31", "6.3.1", "63.1"]:
            self.content = RASMAP_631
        else:
            raise ValueError(f"model version '{self.version}' is not supported")

    def update_projection(self, projection_file: str):
        """
        Add/update the projection file to the RAS Mapper content

        Args:
            projection_file (str): path to projeciton file containing the coordinate system (.prj)

        Raises:
            FileNotFoundError:
        """

        directory = os.path.dirname(self.text_file)
        projection_base = os.path.basename(projection_file)

        if projection_base not in os.listdir(directory):
            raise FileNotFoundError(
                f"Expected projection file to be in RAS directory: {directory}. Provided location is: {projection_file}"
            )

        lines = self.content.splitlines()
        lines.insert(2, rf'  <RASProjectionFilename Filename=".\{projection_base}" />')

        self.content = "\n".join(lines)

    def add_result_layers(self, plan_short_id: str, profiles: list, variable: str):
        """
        Add results layers to RasMap content. When the RAS plan is ran with "Floodplain Mapping" toggled on
        the result layer added here will output rasters.

        Args:
            plan_short_id (str): Plan short id for the output raster(s)
            profiles (list): Profiles for the output raster(s)
            variable (str): Variable to create rasters for. Currently "Depth" is the only supported variable.
        """

        if variable not in ["Depth"]:
            raise NotImplemented(f"Variable {variable} not currently implemented. Currently only Depth is supported.")

        lines = []
        for line in self.content.splitlines():
            if line == "  </Results>":
                for i, profile in enumerate(profiles):
                    lines.append(
                        rf'      <Layer Name="{variable}" Type="RASResultsMap" Checked="True" Filename=".\{plan_short_id}\{variable} ({profile}).vrt">'
                    )
                    lines.append(
                        rf'        <MapParameters MapType="{variable.lower()}" OutputMode="Stored Current Terrain" StoredFilename=".\{plan_short_id}\{variable} ({profile}).vrt" ProfileIndex="{i}" ProfileName="{profile}" />'
                    )
                    lines.append("      </Layer>")
                lines.append("    </Layer>")
            lines.append(line)

        self.content = "\n".join(lines)

    def add_plan_layer(self, plan_short_id: str, plan_hdf: str, profiles: list):
        """
        Add a plan layer to the results in the RASMap content

        Args:
            plan_short_id (str): plan_short_id of the plan
            plan_hdf (str): _description_
            profiles (list): _description_
        """
        lines = []
        for line in self.content.splitlines():

            if line == "  <Results />":

                lines.append(
                    PLAN.replace("plan_hdf_placeholder", plan_hdf)
                    .replace("plan_name_placeholder", plan_short_id)
                    .replace("profile_placeholder", profiles[0])
                )
                continue

            lines.append(line)

        self.content = "\n".join(lines)

    def add_terrain(self):
        """
        Add Terrain to RasMap content
        """

        lines = []
        for line in self.content.splitlines():

            if line == "  <Terrains />":

                lines.append(TERRAIN)
                continue

            lines.append(line)

        self.content = "\n".join(lines)

    def write(self):
        """
        write Ras Map content to file
        """

        print(f"writing: {self.text_file}")

        with open(self.text_file, "w") as f:
            f.write(self.content)

        # write backup
        with open(self.text_file + ".backup", "w") as f:
            f.write(self.content)
