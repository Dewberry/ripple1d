import glob
import logging
import os
import platform
import re
import subprocess
import time
from pathlib import Path
from typing import List

import fiona
import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
from pyproj import CRS

from .consts import (
    FLOW_HDF_PATH,
    MIN_FLOW,
    NORMAL_DEPTH,
    PROFILE_NAMES_HDF_PATH,
    SUPPORTED_LAYERS,
    TERRAIN_NAME,
    TERRAIN_PATH,
    WSE_HDF_PATH,
    XS_NAMES_HDF_PATH,
)
from .data_model import Junction, Reach
from .errors import (
    FlowTitleAlreadyExistsError,
    HECRASVersionNotInstalledError,
    NoCrossSectionLayerError,
    NoRiverLayerError,
    PlanTitleAlreadyExistsError,
    RASComputeTimeoutError,
)
from .rasmap import PLAN, RASMAP_631, TERRAIN
from .utils import (
    assert_no_mesh_error,
    assert_no_ras_compute_error_message,
    assert_no_ras_geometry_error,
    assert_no_store_all_maps_error_message,
    decode,
    get_terrain_exe_path,
    replace_line_in_contents,
    search_contents,
)

if platform.system() == "Windows":
    import win32com.client
    from pythoncom import com_error


RAS_FILE_TYPES = ["Plan", "Flow", "Geometry", "Project"]

VALID_PLANS = [f".p{i:02d}" for i in range(1, 100)]
VALID_GEOMS = [f".g{i:02d}" for i in range(1, 100)]
VALID_STEADY_FLOWS = [f".f{i:02d}" for i in range(1, 100)]
VALID_UNSTEADY_FLOWS = [f".u{i:02d}" for i in range(1, 100)]
VALID_QUASISTEADY_FLOWS = [f".q{i:02d}" for i in range(1, 100)]


# Decorator Functions
def write_new_plan_text_file(func):
    def wrapper(self, *args, **kwargs):

        title = args[0]
        geom_title = args[1]
        if title in self.plans.keys():
            raise PlanTitleAlreadyExistsError(f"The specified plan title {title} already exists")

        func(self, *args, **kwargs)

        # get a new extension number for the new plan
        new_extension_number = get_new_extension_number(self.plans)

        text_file = self.ras_project._ras_root_path + f".p{new_extension_number}"

        # create plan
        plan_text_file = RasPlanText(text_file, self.projection, new_file=True)

        if "write_depth_grids" in kwargs:
            if kwargs["write_depth_grids"]:
                self.update_rasmapper_for_mapping()

            # populate new plan info
            plan_text_file.new_plan_contents(
                title, title, self.flows[title], self.geoms[geom_title], kwargs["write_depth_grids"]
            )

        # write content
        plan_text_file.write_contents()

        # add new plan to the ras class
        self.plans[title] = plan_text_file
        self.plan = plan_text_file

        # add to ras project contents
        self.ras_project.contents.append(f"Plan File=p{new_extension_number}")

        # update the content of the RAS project file
        self.contents = self.ras_project.set_current_plan(self.plans[title].file_extension)

        # write the update RAS project file content
        self.ras_project.write_updated_contents()

        # run the RAS plan
        self.run_sim(close_ras=True, show_ras=True, ignore_store_all_maps_error=True)

    return wrapper


def write_new_flow_text_file(func):
    def wrapper(self, *args, **kwargs):
        title = args[0]
        if title in self.flows.keys():
            raise FlowTitleAlreadyExistsError(f"The specified flow title {title} already exists")

        # get a new extension number for the new flow file
        new_extension_number = get_new_extension_number(self.flows)
        text_file = self.ras_project._ras_root_path + f".f{new_extension_number}"

        # create new flow
        flow_text_file = RasFlowText(text_file, new_file=True)

        # call function
        flow_text_file = func(self, flow_text_file, *args, **kwargs)

        # write flow file content
        flow_text_file.write_contents()

        # add new flow to the ras class
        self.flows[title] = flow_text_file
        self.flow = flow_text_file

        # add to ras project contents
        self.ras_project.contents.append(f"Flow File=f{new_extension_number}")

    return wrapper


def check_projection(func):
    def wrapper(self, *args, **kwargs):
        if self.projection is None:
            raise ValueError("Projection cannot be None")
        return func(self, *args, **kwargs)

    return wrapper


def combine_root_extension(func):
    def wrapper(self, *args, **kwargs):
        extensions = func(self, *args, **kwargs)
        return [self._ras_root_path + "." + extension for extension in extensions]

    return wrapper


def check_version_installed(version: str):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            try:
                assert win32com.client.Dispatch(f"RAS{version}.HECRASCONTROLLER")
                self.version = version
            except com_error:
                raise HECRASVersionNotInstalledError(
                    f"Could not find the specified RAS version; please ensure it is installed. Version provided: {version}."
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


def check_windows(func):
    def wrapper(self, *args, **kwargs):
        if platform.system() != "Windows":
            raise SystemError("This method can only be run on a Windows machine.")
        return func(self, *args, **kwargs)

    return wrapper


def add_fid_index(func):
    def wrapper(*args, **kwargs):
        gdf = func(*args, **kwargs)
        gdf.index.name = "ID"
        return gdf

    return wrapper


# classes
class RasManager:
    def __init__(
        self,
        ras_text_file_path: str,
        version: str = "631",
        terrain_path: str = None,
        projection: CRS = None,
        new_project: bool = False,
    ):

        self.version = version
        self.terrain_path = terrain_path
        self.ras_project = RasProject(ras_text_file_path, new_file=new_project)

        self.projection = CRS(projection)
        self.plans = self.get_plans()
        self.geoms = self.get_geoms()
        self.flows = self.get_flows()
        self.plan = self.current_plan

    def __repr__(self):
        return f"RasManager(project={self.ras_project._ras_text_file_path} ras-version={self.version})"

    @classmethod
    def from_gpkg(
        cls, ras_project_text_file: str, nwm_id, ras_gpkg_file_path: str, version: str = "631", terrain_path: str = None
    ):
        inst = cls(
            ras_project_text_file,
            version,
            terrain_path=terrain_path,
            projection=gpd.read_file(ras_gpkg_file_path).crs,
            new_project=True,
        )

        inst.new_geom_from_gpkg(ras_gpkg_file_path, nwm_id)
        inst.ras_project.write_contents()
        return inst

    @property
    def current_plan(self):
        for plan in self.plans.values():
            if plan.file_extension == self.ras_project.current_plan:
                return plan

    @property
    def projection_file(self):
        projection_file = os.path.join(self.ras_project._ras_dir, "projection.prj")
        with open(projection_file, "w") as f:
            f.write(self.projection.to_wkt("WKT1_ESRI"))
        return projection_file

    def get_plans(self):
        """
        Create plan objects for each plan.
        """
        plans = {}
        for plan_file in self.ras_project.plans:
            try:
                plan = RasPlanText(plan_file, self.projection)
                plans[plan.title] = plan
            except FileNotFoundError:
                logging.info(f"Could not find plan file: {plan_file}")
        return plans

    @check_projection
    def get_geoms(self):
        """
        Create geom objects for each geom.
        """
        geoms = {}
        for geom_file in self.ras_project.geoms:
            try:
                geom = RasGeomText(geom_file, self.projection)
                geoms[geom.title] = geom
            except FileNotFoundError:
                logging.warning(f"Could not find geom file: {geom_file}")
        return geoms

    def get_flows(self):
        """
        Create flow objects for each flow.
        """
        flows = {}
        for flow_file in self.ras_project.steady_flows:
            try:
                flow = RasFlowText(flow_file)
                flows[flow.title] = flow
            except FileNotFoundError:
                logging.warning(f"Could not find flow file: {flow_file}")
        return flows

    @check_windows
    @check_version_installed("631")
    def run_sim(
        self,
        pid_running=None,
        close_ras=True,
        show_ras=False,
        ignore_store_all_maps_error: bool = False,
        timeout_seconds=None,
    ):
        """
        Run the current plan.

        Args:
            pid_running (_type_, optional): _description_. Defaults to None.
            close_ras (bool, optional): boolean to close RAS or not after computing. Defaults to True.
            show_ras (bool, optional): boolean to show RAS or not when computing. Defaults to True.
        """

        compute_message_file = self.ras_project._ras_root_path + f"{self.plan.file_extension}.computeMsgs.txt"

        RC = win32com.client.Dispatch(f"RAS{self.version}.HECRASCONTROLLER")
        try:
            RC.Project_Open(self.ras_project._ras_text_file_path)
            if show_ras:
                RC.ShowRas()

            RC.Compute_CurrentPlan()
            deadline = (timeout_seconds + time.time()) if timeout_seconds else float("inf")
            while not RC.Compute_Complete():
                if time.time() > deadline:
                    raise RASComputeTimeoutError(
                        f"timed out computing current plan for RAS project: {self.ras_project._ras_text_file_path}"
                    )
                # must keep checking for mesh errors while RAS is running
                # because mesh error will cause blocking popup message
                assert_no_mesh_error(compute_message_file, require_exists=False)  # file might not exist immediately
                time.sleep(0.2)
            time.sleep(
                3
            )  # ample sleep to account for race condition of RAS flushing final lines to compute_message_file

            assert_no_mesh_error(compute_message_file, require_exists=True)
            assert_no_ras_geometry_error(compute_message_file)
            assert_no_ras_compute_error_message(compute_message_file)
            if not ignore_store_all_maps_error:
                assert_no_store_all_maps_error_message(compute_message_file)
        finally:
            if close_ras:
                RC.Project_Close()
                RC.QuitRas()

    @write_new_plan_text_file
    @write_new_flow_text_file
    def normal_depth_run(
        self,
        flow_text_file,
        title: str,
        geom_title: str,
        flows: list[float],
        river: str,
        reach: str,
        us_river_station: float,
        normal_depth: float = NORMAL_DEPTH,
        write_depth_grids: bool = False,
    ):

        flows = [int(max([i, MIN_FLOW])) for i in flows]

        # write headers
        flow_text_file.contents += flow_text_file.write_headers(title, flows)

        # write discharges
        flow_text_file.contents += flow_text_file.write_discharges(flows, river, reach, us_river_station)

        # write normal depth
        flow_text_file.contents += flow_text_file.write_ds_normal_depth(len(flows), normal_depth, river, reach)

        return flow_text_file

    @write_new_plan_text_file
    @write_new_flow_text_file
    def kwses_run(
        self,
        flow_text_file,
        title: str,
        geom_title: str,
        depths: List[float],
        wses: List[float],
        flows: List[float],
        river: str,
        reach: str,
        us_river_station: float,
        ds_river_station: float,
        write_depth_grids: bool = False,
    ):
        flows = [int(max([i, MIN_FLOW])) for i in flows]
        profile_names = [f"f_{flow}-z_{str(depth).replace('.','_')}" for flow, depth in zip(flows, depths)]

        # write headers
        flow_text_file.contents += flow_text_file.write_headers(title, profile_names)

        # write discharges
        flow_text_file.contents += flow_text_file.write_discharges(flows, river, reach, us_river_station)

        # write DS boundary conditions
        flow_text_file.contents += flow_text_file.write_ds_known_ws(wses, len(flows), river, reach, ds_river_station)

        return flow_text_file

    def new_geom_from_gpkg(
        self,
        ras_gpkg_file_path: str,
        title: str,
    ):
        new_extension_number = get_new_extension_number(self.geoms)
        text_file = self.ras_project._ras_root_path + f".g{new_extension_number}"
        geom_text_file = RasGeomText.from_gpkg(ras_gpkg_file_path, title, self.version, text_file)
        geom_text_file.write_contents()
        self.geoms[geom_text_file.title] = geom_text_file
        self.ras_project.contents.append(f"Geom File=g{new_extension_number}")

    def update_rasmapper_for_mapping(self):
        """
        Write a rasmapper file to output depth grids for the current plan
        """

        # manage rasmapper
        map_file = f"{self.ras_project._ras_root_path}.rasmap"

        if os.path.exists(map_file):
            os.remove(map_file)

        if os.path.exists(map_file + ".backup"):
            os.remove(map_file + ".backup")

        terrain_relative_path = os.path.relpath(self.terrain_path, self.ras_project._ras_dir)
        terrain_name = os.path.splitext(os.path.basename(self.terrain_path))[0]

        rasmap = RasMap(map_file, self.plan.geom, self.version)
        rasmap.update_projection(self.projection_file)
        rasmap.add_terrain(terrain_name, terrain_relative_path)
        rasmap.add_plan_layer(self.plan.title, os.path.basename(self.plan.hdf_file), self.plan.flow.profile_names)
        rasmap.add_result_layers(self.plan.title, self.plan.flow.profile_names, "Depth")
        rasmap.write()

        return self


class RasTextFile:
    def __init__(self, ras_text_file_path, new_file=False):
        self._ras_text_file_path = ras_text_file_path

        if not new_file and not os.path.exists(ras_text_file_path):
            raise FileNotFoundError(f"could not find {ras_text_file_path}")
        else:
            self._ras_text_file_path = ras_text_file_path
            self._ras_root_path = os.path.splitext(self._ras_text_file_path)[0]

        if not new_file:
            self.read_contents()
        else:
            self.contents = []

    def __repr__(self):
        return f"RasTextFile({self._ras_text_file_path})"

    def read_contents(self):
        if not os.path.exists(self._ras_text_file_path):
            raise FileNotFoundError(f"could not find {self._ras_text_file_path}")
        with open(self._ras_text_file_path) as f:
            self.contents = f.read().splitlines()

    def write_contents(self):
        if os.path.exists(self._ras_text_file_path):
            raise FileExistsError(f"The specified file already exists {self._ras_text_file_path}")

        logging.info(f"writing: {self._ras_text_file_path}")
        with open(self._ras_text_file_path, "w") as f:
            f.write("\n".join(self.contents))

    def write_updated_contents(self):
        if not os.path.exists(self._ras_text_file_path):
            raise FileNotFoundError(f"The specified file doesn't exists {self._ras_text_file_path}")

        logging.info(f"updating: {self._ras_text_file_path}")
        with open(self._ras_text_file_path, "w") as f:
            f.write("\n".join(self.contents))

    @property
    def file_extension(self):
        return Path(self._ras_text_file_path).suffix


class RasProject(RasTextFile):
    def __init__(self, ras_text_file_path: str, new_file: bool = False):
        super().__init__(ras_text_file_path, new_file)
        if self.file_extension != ".prj":
            raise TypeError(f"Plan extenstion must be .prj, not {self.file_extension}")

        self._ras_project_basename = os.path.splitext(os.path.basename(self._ras_text_file_path))[0]
        self._ras_dir = os.path.dirname(self._ras_text_file_path)
        os.makedirs(self._ras_dir, exist_ok=True)

        if new_file:
            self.contents = [f"Proj Title={self._ras_project_basename}", "Current Plan="]

    def __repr__(self):
        return f"RasProject({self._ras_text_file_path})"

    @property
    def title(self):
        return search_contents(self.contents, "Proj Title")

    @property
    @combine_root_extension
    def plans(self):
        return search_contents(self.contents, "Plan File", expect_one=False)

    @property
    @combine_root_extension
    def geoms(self):
        return search_contents(self.contents, "Geom File", expect_one=False)

    @property
    @combine_root_extension
    def unsteady_flows(self):
        return search_contents(self.contents, "Unsteady File", expect_one=False)

    @property
    @combine_root_extension
    def steady_flows(self):
        return search_contents(self.contents, "Flow File", expect_one=False)

    @property
    def current_plan(self):
        return search_contents(self.contents, "Current Plan")

    def set_current_plan(self, plan_ext):
        """
        Return new contents with the specified plan as the current RAS plan.

        Args:
            plan_ext: The plan extension to set as the current plan
        """
        new_contents = self.contents
        if f"{plan_ext}" not in VALID_PLANS:
            raise TypeError(f"Plan extenstion must be one of .p01-.p99, not {plan_ext}")
        else:
            new_contents = replace_line_in_contents(new_contents, "Current Plan", plan_ext)

        # TODO: Update this to put it with the other plans
        if f"Plan File={plan_ext}" not in new_contents:
            new_contents.append(f"Plan File={plan_ext}")
        logging.info("set plan!")
        return new_contents


class RasPlanText(RasTextFile):
    def __init__(self, ras_text_file_path: str, projection: str = None, new_file: bool = False):
        super().__init__(ras_text_file_path, new_file)
        if self.file_extension not in VALID_PLANS:
            raise TypeError(f"Plan extenstion must be one of .p01-.p99, not {self.file_extension}")
        self.projection = projection
        self.hdf_file = self._ras_text_file_path + ".hdf"

    def __repr__(self):
        return f"RasPlanText({self._ras_text_file_path})"

    @property
    def title(self):
        return search_contents(self.contents, "Plan Title")

    @property
    def version(self):
        return search_contents(self.contents, "Program Version")

    @property
    def plan_geom_file(self):
        return search_contents(self.contents, "Geom File")

    @property
    def plan_unsteady_flow(self):
        return search_contents(self.contents, "Unsteady File")

    @property
    def plan_steady_flow(self):
        return search_contents(self.contents, "Flow File")

    @property
    @check_projection
    def geom(self):
        return RasGeomText(
            f"{os.path.splitext(self._ras_text_file_path)[0]}.{self.plan_geom_file}",
            self.projection,
        )

    @property
    def flow(self):
        return RasFlowText(f"{os.path.splitext(self._ras_text_file_path)[0]}.{self.plan_steady_flow}")

    def new_plan_from_existing(self, title, short_id, geom_ext, flow_ext):
        """
        Populate the content of the new plan with basic attributes (title, short_id, flow, and geom)
        """
        new_contents = self.contents
        if len(title) > 80:
            raise ValueError("Short Identifier must be less than 80 characters")
        else:
            new_contents = replace_line_in_contents(new_contents, "Plan Title", title)

        if len(short_id) > 80:
            raise ValueError("Short Identifier must be less than 80 characters")
        else:
            new_contents = replace_line_in_contents(new_contents, "Short Identifier", short_id)

        if f".{geom_ext}" not in VALID_GEOMS:
            raise TypeError(f"Geometry extenstion must be one of g01-g99, not {geom_ext}")
        else:
            new_contents = replace_line_in_contents(new_contents, "Geom File", geom_ext)

        if f".{flow_ext}" not in VALID_STEADY_FLOWS:
            raise TypeError(f"Flow extenstion must be one of f01-f99, not {flow_ext}")
        else:
            new_contents = replace_line_in_contents(new_contents, "Flow File", flow_ext)

        try:
            new_contents = replace_line_in_contents(new_contents, "Run RASMapper", "-1")
        except ValueError:
            new_contents.append("Run RASMapper=-1 ")

        return new_contents

    def new_plan_contents(self, title: str, short_id: str, flow, geom, run_rasmapper: bool = False):
        """
        populate the content of the plan with basic attributes (title, short_id, flow, and geom)

        Raises:
            RuntimeError: raise run time error if the plan already has content associated with it
        """

        if self.contents:
            raise RuntimeError(f"content already exists for this plan: {self._ras_text_file_path}")

        # create necessary lines for the content of the plan text file.
        if len(title) > 80:
            raise ValueError("Short Identifier must be less than 80 characters")
        else:
            self.contents.append(f"Plan Title={title}")
        if len(short_id) > 80:
            raise ValueError("Short Identifier must be less than 80 characters")
        else:
            self.contents.append(f"Short Identifier={short_id}")

        if f"{geom.file_extension}" not in VALID_GEOMS:
            raise TypeError(f"Geometry extenstion must be one of g01-g99, not {geom.file_extension}")
        else:
            self.contents.append(f"Geom File={geom.file_extension.lstrip('.')}")

        if f"{flow.file_extension}" not in VALID_STEADY_FLOWS:
            raise TypeError(f"Flow extenstion must be one of f01-f99, not {flow.file_extension}")
        else:
            self.contents.append(f"Flow File={flow.file_extension.lstrip('.')}")
        if run_rasmapper:
            self.contents.append("Run RASMapper=-1 ")
        else:
            self.contents.append("Run RASMapper= 0 ")

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

            # get columns and indexes for the wse and flow arrays
            columns = decode(pd.DataFrame(hdf[XS_NAMES_HDF_PATH]))
            index = decode(pd.DataFrame(hdf[PROFILE_NAMES_HDF_PATH]))

            # remove multiple spaces in between the river-reach-riverstation ids.
            columns[0] = columns[0].apply(remove_multiple_spaces)

            # create dataframes for the wse and flow results
            wse = pd.DataFrame(hdf.get(WSE_HDF_PATH), columns=columns[0].values, index=index[0].values).T
            flow = pd.DataFrame(hdf.get(FLOW_HDF_PATH), columns=columns[0].values, index=index[0].values).T

        return wse, flow


class RasGeomText(RasTextFile):
    def __init__(self, ras_text_file_path: str, projection: str = None, new_file=False):
        super().__init__(ras_text_file_path, new_file)
        if not new_file and self.file_extension not in VALID_GEOMS:
            raise TypeError(f"Geometry extenstion must be one of .g01-.g99, not {self.file_extension}")

        self.projection = CRS(projection)
        self.hdf_file = self._ras_text_file_path + ".hdf"

    def __repr__(self):
        return f"RasGeomText({self._ras_text_file_path})"

    @classmethod
    def from_str(cls, text_string: str, projection, ras_text_file_path: str = ""):
        inst = cls("", projection, new_file=True)
        inst.contents = text_string.splitlines()
        return inst

    @classmethod
    def from_gpkg(cls, gpkg_path, title: str, version: str, ras_text_file_path: str = ""):

        inst = cls(ras_text_file_path, projection=gpd.read_file(gpkg_path).crs, new_file=True)
        inst._gpkg_path = gpkg_path
        inst._version = version
        inst._title = title

        inst.contents = inst._content_from_gpkg
        return inst

    @property
    def _content_from_gpkg(
        self,
    ):

        if not hasattr(self, "_gpkg_path"):
            raise ("gpkg_path not provided")
        if not os.path.exists(self._gpkg_path):
            raise FileNotFoundError(f"Could not find the specified gpkg_path {self._gpkg_path}")

        self._check_layers()

        xs_gdf = gpd.read_file(self._gpkg_path, layer="XS", driver="GPKG")

        # headers
        gpkg_data = (
            f"Geom Title={self._title}\nProgram Version={self._version}\nViewing Rectangle={self._bbox(xs_gdf)}\n\n"
        )

        # junction data
        if "Junction" in fiona.listlayers(self._gpkg_path):
            junction_gdf = gpd.read_file(self._gpkg_path, layer="Junction", driver="GPKG")
            junction_gdf["ras_data"].str.cat(sep="\n")

        # river reach data
        gpkg_data += self._river_reach_data_from_gpkg

        # cross section data
        gpkg_data += xs_gdf["ras_data"].str.cat(sep="\n")

        return gpkg_data.splitlines()

    @property
    def _river_reach_data_from_gpkg(self):
        river_gdf = gpd.read_file(self._gpkg_path, layer="River", driver="GPKG").iloc[0]
        centroid = river_gdf.geometry.centroid

        coords = river_gdf["geometry"].coords
        data = f"River Reach={river_gdf['river'].ljust(16)},{river_gdf['reach'].ljust(16)}\n"
        data += f"Reach XY= {len(coords)} \n"

        for i, (x, y) in enumerate(coords):
            data += str(x).rjust(16) + str(y).rjust(16)
            if i % 2 != 0:
                data += "\n"

        data += f"\nRch Text X Y={centroid.x},{centroid.y}\nReverse River Text= 0 \n\n"

        return data

    def _bbox(self, gdf):
        bounds = gdf.total_bounds
        return f"{bounds[0]} , {bounds[2]} , {bounds[3]} , {bounds[1]} "

    def _check_layers(self):
        layers = set(fiona.listlayers(self._gpkg_path)) & set(SUPPORTED_LAYERS)

        if "XS" not in layers:
            raise NoCrossSectionLayerError(f"Could not find a layer called XS in {self._gpkg_path}")

        if "River" not in layers:
            raise NoRiverLayerError(f"Could not find a layer called River in {self._gpkg_path}")

    @property
    def title(self):
        return search_contents(self.contents, "Geom Title")

    @property
    def version(self):
        return search_contents(self.contents, "Program Version")

    @property
    @check_projection
    def reaches(self) -> dict:
        river_reaches = search_contents(self.contents, "River Reach", expect_one=False)
        reaches = {}
        for river_reach in river_reaches:
            reaches[river_reach] = Reach(self.contents, river_reach, self.projection)
        return reaches

    @property
    @check_projection
    def junctions(self) -> dict:
        juncts = search_contents(self.contents, "Junct Name", expect_one=False)
        junctions = {}
        for junct in juncts:
            junctions[junct] = Junction(self.contents, junct, self.projection)
        return junctions

    @property
    @check_projection
    def cross_sections(self) -> dict:
        cross_sections = {}
        for reach in self.reaches.values():
            cross_sections.update(reach.cross_sections)

        return cross_sections

    @property
    @check_projection
    @add_fid_index
    def reach_gdf(self):
        return pd.concat([reach.gdf for reach in self.reaches.values()], ignore_index=True)

    @property
    @check_projection
    @add_fid_index
    def junction_gdf(self):
        return pd.concat([junction.gdf for junction in self.junctions.values()], ignore_index=True)

    @property
    @check_projection
    @add_fid_index
    def xs_gdf(self):
        """
        Geodataframe of all cross sections in the geometry text file.
        """
        return pd.concat([xs.gdf for xs in self.cross_sections.values()], ignore_index=True)

    def to_gpkg(self, gpkg_path: str):
        self.xs_gdf.to_file(gpkg_path, driver="GPKG", layer="XS")
        self.reach_gdf.to_file(gpkg_path, driver="GPKG", layer="River")
        if self.junctions:
            self.junction_gdf.to_file(gpkg_path, diver="GPKG", layer="Junction")


class RasFlowText(RasTextFile):
    def __init__(self, ras_text_file_path: str, new_file: bool = False):
        super().__init__(ras_text_file_path, new_file)
        if self.file_extension in VALID_UNSTEADY_FLOWS or self.file_extension in VALID_QUASISTEADY_FLOWS:
            raise NotImplementedError("only steady flow (f.**) supported")

        if self.file_extension not in VALID_STEADY_FLOWS:
            raise TypeError(f"Flow extenstion must be one of .f01-.f99, not {self.file_extension}")

    def __repr__(self):
        return f"RasFlowText({self._ras_text_file_path})"

    @property
    def title(self):
        return search_contents(self.contents, "Flow Title")

    @property
    def version(self):
        return search_contents(self.contents, "Program Version")

    @property
    def n_profiles(self):
        return int(search_contents(self.contents, "Number of Profiles"))

    @property
    def profile_names(self):
        return search_contents(self.contents, "Profile Names").split(",")

    @property
    def flow_change_locations(self):
        search_contents(self.contents, "Boundary for River Rch & Prof#", expect_one=False)

    def write_headers(self, title: str, profile_names: list[str]):
        """
        Write headers for flow content

        Args:
            title (str): title of the flow
            profile_names (list[str]): profile names for the flow

        Returns:
            list (list[str]): lines of the flow content
        """
        lines = [
            f"Flow Title={title}",
            f"Number of Profiles= {len(profile_names)}",
            f"Profile Names={','.join([str(pn) for pn in profile_names])}",
        ]
        return lines

    def write_discharges(self, flows: list, river: str, reach: str, river_station: float):
        """
        Write discharges to flow content

        Args:
            flows (list): flows to write to the flow content
            river (str): Ras river
            reach (str): Ras reach
            river_station (float): Ras river station
        """

        lines = []
        lines.append(
            f"River Rch & RM={river},{reach.ljust(16,' ')},{str(river_station).rstrip('0').rstrip('.').ljust(8,' ')}"
        )
        line = ""
        for i, flow in enumerate(flows):
            line += f"{str(max([int(flow),MIN_FLOW])).rjust(8,' ')}"
            if (i + 1) % 10 == 0:
                lines.append(line)
                line = ""

        if (i + 1) % 10 != 0:
            lines.append(line)
        return lines

    def write_ds_known_ws(
        self, ds_wses: list[float], number_of_flows: float, river: str, reach: str, river_station: float
    ):
        """
        Write downstream known water surface elevations to flow content

        Args:
            ds_wses (list): downstream known water surface elevations
            number_of_flows (float): number of flows that are applied
            river (str): Ras river
            reach (str): Ras reach
            river_station (float): Ras river station
        """
        count = 0
        lines = []
        for wse in ds_wses:
            for _ in range(number_of_flows):
                count += 1
                lines.append(f"Boundary for River Rch & Prof#={river},{reach.ljust(16,' ')}, {count}")
                lines.append("Up Type= 0 ")
                lines.append("Dn Type= 1 ")
                lines.append(f"Dn Known WS={wse}")

        return lines

    def write_ds_normal_depth(self, number_of_profiles: int, normal_depth: float, river: str, reach: str):
        """
        Write the downstream normal depth boundary condition

        Args:
            number_of_profiles (int): Number of profiles
            normal_depth (float): Normal depth slope to apply to all profiles
            river (str): Ras river
            reach (str): Ras reach
        """
        lines = []
        for i in range(number_of_profiles):
            lines.append(f"Boundary for River Rch & Prof#={river},{reach.ljust(16,' ')}, {i+1}")
            lines.append("Up Type= 0 ")
            lines.append("Dn Type= 3 ")
            lines.append(f"Dn Slope={normal_depth}")

        return lines


class RasMap:
    """
    Represents a single RasMapper file.

    Attributes
    ----------
    text_file : Text file repressenting the RAS Mapper file
    contents : a text representation of the files contents
    version : HEC-RAS version for this RAS Mapper file

    """

    def __init__(self, path: str, geom, version: str = 631):
        """
        Args:
            path (str): file path to the plan text file
            version (str): HEC-RAS vesion for this RAS Mapper file
        """
        self.text_file = path
        self.version = version

        if os.path.exists(path):
            self.read_contents()
        else:
            self.new_rasmap_content(geom)

    def __repr__(self):
        return f"RasMap({self.path})"

    def read_contents(self):
        """
        Read contents of the file. Searches for the file locally and on s3.

        Raises:
            FileNotFoundError:
        """
        if os.path.exists(self.text_file):
            with open(self.text_file) as f:
                self.contents = f.read()
        else:
            raise FileNotFoundError(f"could not find {self.text_file}")

    def new_rasmap_content(self, geom):
        """
        Populate the contents with boiler plate contents
        """
        if self.version in ["631", "6.31", "6.3.1", "63.1"]:
            self.contents = RASMAP_631.replace("geom_hdf_placeholder", os.path.basename(geom.hdf_file)).replace(
                "geom_name_placeholder", geom.title
            )
        else:
            raise ValueError(f"model version '{self.version}' is not supported")

    def update_projection(self, projection_file: str):
        """
        Add/update the projection file to the RAS Mapper contents

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

        lines = self.contents.splitlines()
        lines.insert(2, rf'  <RASProjectionFilename Filename=".\{projection_base}" />')

        self.contents = "\n".join(lines)

    def add_result_layers(self, plan_short_id: str, profiles: list[str], variable: str):
        """
        Add results layers to RasMap contents. When the RAS plan is ran with "Floodplain Mapping" toggled on
        the result layer added here will output rasters.

        Args:
            plan_short_id (str): Plan short id for the output raster(s)
            profiles (list[str]): Profiles for the output raster(s)
            variable (str): Variable to create rasters for. Currently "Depth" is the only supported variable.
        """

        if variable not in ["Depth"]:
            raise NotImplementedError(
                f"Variable {variable} not currently implemented. Currently only Depth is supported."
            )

        lines = []
        for line in self.contents.splitlines():
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

        self.contents = "\n".join(lines)

    def add_plan_layer(self, plan_short_id: str, plan_hdf: str, profiles: list[str]):
        """
        Add a plan layer to the results in the RASMap contents

        Args:
            plan_short_id (str): plan_short_id of the plan
            plan_hdf (str): hdf file for the plan
            profiles (list[str]): profiles for the plan
        """
        lines = []
        for line in self.contents.splitlines():

            if line == "  <Results />":

                lines.append(
                    PLAN.replace("plan_hdf_placeholder", plan_hdf)
                    .replace("plan_name_placeholder", str(plan_short_id))
                    .replace("profile_placeholder", profiles[0])
                )
                continue

            lines.append(line)

        self.contents = "\n".join(lines)

    def add_terrain(self, terrain_name: str, terrain_path: str):
        """
        Add Terrain to RasMap content
        """

        lines = []
        for line in self.contents.splitlines():

            if line == "  <Terrains />":

                lines.append(TERRAIN.replace(TERRAIN_NAME, terrain_name).replace(TERRAIN_PATH, terrain_path))
                continue

            lines.append(line)

        self.contents = "\n".join(lines)

    def write(self):
        """
        write Ras Map contents to file
        """

        logging.info(f"writing: {self.text_file}")

        with open(self.text_file, "w") as f:
            f.write(self.contents)

        # write backup
        with open(self.text_file + ".backup", "w") as f:
            f.write(self.contents)


# functions
def search_for_ras_projection(search_dir: str):
    rasmap_files = glob.glob(f"{search_dir}/*.rasmap")
    if rasmap_files:
        rm = rasmap_files[0]
    else:
        raise FileNotFoundError(f"Could not find rasmap file in {search_dir}")

    with open(rm) as f:
        for line in f.readlines():
            if "RASProjectionFilename Filename=" in line:
                relative_path = line.split("=")[-1].split('"')[1].lstrip(".")
                abs_path = f"{search_dir}/{relative_path}"
                if os.path.exists(abs_path):
                    with open(abs_path) as src:
                        projection = src.read()
                        return projection, abs_path
                else:
                    raise FileNotFoundError(f"Could not find projection file in {search_dir}")


def get_new_extension_number(dict_of_ras_subclasses: dict) -> str:
    """
    Determines the next numeric extension that should be used when creating a new plan, flow, or geom;
    e.g., if you are adding a new plan and .p01, and .p02 already exists then the new plan
    will have a .p03 extension.

    Args:
        dict_of_ras_subclasses (dict): A dictionary containing plan/geom/flow titles as keys
            and objects plan/geom/flow as values.

    Returns:
        new file extension (str): The new file exension.
    """
    extension_number = []
    if not dict_of_ras_subclasses:
        return "01"
    for val in dict_of_ras_subclasses.values():
        extension_number.append(int(val.file_extension[2:]))

    return f"{(max(extension_number)+1):02d}"


def create_terrain(
    src_terrain_filepaths: list[str],
    projection_file: str,
    terrain_hdf_filepath: str,
    vertical_units: str = "Feet",
    version: str = "631",
) -> str:
    r"""
    Uses the projection file and a list of terrain file paths to make the RAS terrain HDF file.
    Default location is {model_directory}\Terrain\Terrain.hdf.

    Returns the full path to the local directory containing the output files.

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

    terrain_exe = get_terrain_exe_path(version)
    if not os.path.isfile(terrain_exe):
        raise FileNotFoundError(terrain_exe)

    exe_parent_dir = os.path.split(terrain_exe)[0]

    subproc_args = [
        terrain_exe,
        "CreateTerrain",
        f"units={vertical_units}",  # vertical units
        "stitch=true",
        f"prj={projection_file}",
        f"out={terrain_hdf_filepath}",
    ]
    # add list of input rasters from which to build the Terrain
    subproc_args.extend([os.path.abspath(p) for p in src_terrain_filepaths])
    logging.debug(f"Running the following args, from {exe_parent_dir}:" + "\n  ".join([""] + subproc_args))
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
