import glob
import os
import platform
import time
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from pyproj import CRS

from .consts import TERRAIN_NAME
from .data_model import Reach
from .errors import (
    FlowTitleAlreadyExistsError,
    HECRASVersionNotInstalledError,
    PlanTitleAlreadyExistsError,
    RASComputeTimeoutError,
)
from .rasmap import PLAN, RASMAP_631, TERRAIN
from .utils import (
    assert_no_mesh_error,
    assert_no_ras_compute_error_message,
    assert_no_ras_geometry_error,
    assert_no_store_all_maps_error_message,
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


class RasManager:
    def __init__(self, ras_text_file_path: str, version: str = "631", projection: str = None):
        self._ras_text_file_path = ras_text_file_path

        self._ras_project_basename = os.path.splitext(os.path.basename(self._ras_text_file_path))[0]
        self._ras_dir = os.path.dirname(self._ras_text_file_path)

        self.ras_project = RasProject(self._ras_text_file_path)
        self.projection = "EPSG:4269"
        self.plans = self.get_plans()
        self.geoms = self.get_geoms()
        self.flows = self.get_flows()
        # self.get_active_plan()

    def get_plans(self):
        """
        Create plan objects for each plan.
        """
        plans = {}
        for plan_file in self.ras_project.plans:
            try:
                plan = RasPlanText(plan_file)
                plans[plan.title] = plan
            except FileNotFoundError:
                print(f"Could not find plan file: {plan_file}")
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
                print(f"Could not find geom file: {geom_file}")
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
                print(f"Could not find flow file: {flow_file}")
        return flows

    def set_active_plan_for_ras_manager(self):
        """
        Reads the content of the RAS project file to determine what the current
            plan is and sets the asociated plans,geoms,and flows as active.
        """
        current_plan_extension = search_contents(self.ras_project.contents, "Current Plan")

        for plan in self.plans.values():
            if plan.file_extension == f".{current_plan_extension}":
                self.plan = plan
                self.geom = plan.geom
                self.flow = plan.flow

    def update_content(self):
        pass

    def write_to_new_file(self):
        pass

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
            RC.Project_Open(self._ras_text_file_path)
            if show_ras:
                RC.ShowRas()

            RC.Compute_CurrentPlan()
            deadline = (timeout_seconds + time.time()) if timeout_seconds else float("inf")
            while not RC.Compute_Complete():
                if time.time() > deadline:
                    raise RASComputeTimeoutError(
                        f"timed out computing current plan for RAS project: {self._ras_text_file_path}"
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

    def write_new_flow_initial_normal_depth(self, title: str, branch_data: dict, normal_depth: float):
        """
        Write a new flow file contaning the specified title, branch_data, and normal depth.

        Args:
            title (str): Title of the flow file
            branch_data (pd.DataFrame) dataframe containing rows for each flow change location
                and columns for river,reach,us_rs,ds_rs, and flows.
            normal_depth (float): normal_depth to apply
        """
        if title in self.flows.keys():
            raise FlowTitleAlreadyExistsError(f"The specified flow title {title} already exists")

        # get a new extension number for the new flow file
        new_extension_number = self.get_new_extension_number(self.flows)
        text_file = self.ras_project_file.rstrip(".prj") + f".f{new_extension_number}"

        # create new flow
        flow = RasFlowText(text_file, new_file=True)

        # populate new flow
        flow.write_initial_normal_depth_flow_file(
            title,
            branch_data["flows_rc"],
            normal_depth,
            branch_data["upstream_data"]["river"],
            branch_data["upstream_data"]["reach"],
            branch_data["upstream_data"]["xs_id"],
            branch_data["downstream_data"]["river"],
            branch_data["downstream_data"]["reach"],
        )

        # add new flow to the ras class
        self.flows[title] = flow

    def write_new_flow_file_for_kwses_run(self, title: str, branch_data: dict):
        """
        Write a new flow file contaning the specified title and branch_data.

        Args:
            title (str): Title of the flow file
            branch_data (pd.dict) dictionary containing information for boundary conditions
        """
        if title in self.flows.keys():
            raise FlowTitleAlreadyExistsError(f"The specified flow title {title} already exists")

        # get a new extension number for the new flow file
        new_extension_number = self.get_new_extension_number(self.flows)
        text_file = self.ras_project_file.rstrip(".prj") + f".f{new_extension_number}"

        # create new flow class
        flow = RasFlowText(text_file, new_file=True)

        # populate new flow
        flow.write_kwses_flow_file(
            title,
            branch_data["ds_depths"],
            branch_data["ds_wses"],
            branch_data["us_flows"],
            branch_data["nd_depths"],
            branch_data["upstream_data"]["river"],
            branch_data["upstream_data"]["reach"],
            branch_data["upstream_data"]["xs_id"],
            branch_data["downstream_data"]["river"],
            branch_data["downstream_data"]["reach"],
            branch_data["downstream_data"]["xs_id"],
        )

        # add new flow to the ras manager
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
        if title in self.plans.keys():
            raise PlanTitleAlreadyExistsError(f"The specified plan title {title} already exists")

        # get a new extension number for the new plan
        new_extension_number = self.get_new_extension_number(self.plans)

        text_file = self.ras_project_file.rstrip(".prj") + f".p{new_extension_number}"

        # create plan
        plan = RasPlanText(text_file, self.projection, new_file=True)

        # populate new plan info
        plan.new_plan(geom, flow, title, short_id)

        # write content
        plan.write()

        # add new plan to the ras class
        self.plans[title] = plan


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

    def read_contents(self):
        if not os.path.exists(self._ras_text_file_path):
            raise FileNotFoundError(f"could not find {self._ras_text_file_path}")
        with open(self._ras_text_file_path) as f:
            self.contents = f.read().splitlines()

    @property
    def file_extension(self):
        return Path(self._ras_text_file_path).suffix

    def write(self):
        """
        Write the content to file
        TODO: require file name argument for safety?
        """
        raise NotImplementedError
        # print(f"writing: {self.text_file}")

        # with open(self.text_file, "w") as src:
        #     src.write(self.content)


class RasProject(RasTextFile):
    def __init__(self, ras_text_file_path: str):
        super().__init__(ras_text_file_path)
        if self.file_extension != ".prj":
            raise TypeError(f"Plan extenstion must be .prj, not {self.file_extension}")

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

    def set_current_plan_in(self, current_plan):
        """
        Update the current RAS plan in the RAS project content.
        This does not update the actual file; use the 'write' method to do this.

        Args:
            current_plan (Plan): The plan to set as the current plan
        """

        lines = []
        for line in self.contents.splitlines():
            if "Current Plan=" in line:
                line = f"Current Plan={current_plan.file_extension.lstrip('.')}"
            lines.append(line)

        self.contents = "\n".join(lines)
        # self.write
        # self.plan = current_plan
        raise NotImplementedError


class RasPlanText(RasTextFile):
    def __init__(self, ras_text_file_path: str, projection: str = None, new_file: bool = False):
        super().__init__(ras_text_file_path, new_file)
        if self.file_extension not in VALID_PLANS:
            raise TypeError(f"Plan extenstion must be one of .p01-.p99, not {self.file_extension}")
        self.projection = projection

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

    def populate_new_plan_content(self, title, short_id, geom, flow):
        """
        Populate the content of the new plan with basic attributes (title, short_id, flow, and geom)
        """
        # create necessary lines for the content of the plan text file.
        lines = [
            f"Plan Title={title}",
            f"Short Identifier={short_id}",
            f"Geom File={geom.extension.lstrip('.')}",
            f"Flow File={flow.extension.lstrip('.')}",
            "Run RASMapper=-1 ",
        ]
        return "\n".join(lines)

    def write_new_plan(self):
        pass


class RasGeomText(RasTextFile):
    def __init__(self, ras_text_file_path: str, projection: str = None):
        super().__init__(ras_text_file_path)
        if self.file_extension not in VALID_GEOMS:
            raise TypeError(f"Geometry extenstion must be one of .g01-.g99, not {self.file_extension}")

        self.projection = projection

    @property
    def title(self):
        return search_contents(self.contents, "Geom Title")

    @property
    def version(self):
        return search_contents(self.contents, "Program Version")

    @property
    def river_reaches(self):
        return search_contents(self.contents, "River Reach", expect_one=False)

    @property
    @check_projection
    def reaches(self):
        reaches = []
        for river_reach in self.river_reaches:
            reaches.append(Reach(self.contents, river_reach, self.projection))
        return reaches

    @property
    @check_projection
    def reaches_gdf(self):
        return pd.concat([reach.gdf for reach in self.reaches])

    @check_projection
    def xs_gdf(self):
        """
        Geodataframe of all cross sections in the geometry text file.
        """
        raise NotImplementedError


class RasFlowText(RasTextFile):
    def __init__(self, ras_text_file_path: str, new_file: bool = False):
        super().__init__(ras_text_file_path, new_file)
        if self.file_extension in VALID_UNSTEADY_FLOWS or self.file_extension in VALID_QUASISTEADY_FLOWS:
            raise NotImplementedError("only steady flow (f.**) supported")

        if self.file_extension not in VALID_STEADY_FLOWS:
            raise TypeError(f"Flow extenstion must be one of .f01-.f99, not {self.file_extension}")

    @property
    def title(self):
        return search_contents(self.contents, "Flow Title")

    @property
    def version(self):
        return search_contents(self.contents, "Program Version")

    @property
    def n_profiles(self):
        return int(search_contents(self.contents, "Number of Profiles"))

    def parse_attrs(self):
        raise NotImplementedError

    def parse_flows(self):
        raise NotImplementedError

    def max_flow_applied(self):
        raise NotImplementedError

    # TODO: Should these be functions outside of the class?
    def write_initial_normal_depth_flow_file(
        self,
        title: str,
        us_flows: list[float],
        normal_depth: float,
        us_river: str,
        us_reach: str,
        us_river_station: float,
        ds_river: str,
        ds_reach: str,
    ):
        self.content = ""

        flows = [int(i) for i in us_flows]

        # write headers
        self.write_headers(title, flows)

        self.profile_names = flows
        self.profile_count = len(self.profile_names)
        self.title = title

        # write discharges
        self.write_discharges(flows, us_river, us_reach, us_river_station)

        # write normal depth
        self.write_ds_normal_depth(len(flows), normal_depth, ds_river, ds_reach)

        # write flow file content
        self.write()

    # TODO: Should these be functions outside of the class?
    def write_kwses_flow_file(
        self,
        title: str,
        ds_depths: List[float],
        ds_wses: List[float],
        us_flows: List[float],
        min_depths: pd.Series,
        us_river: str,
        us_reach: str,
        us_river_station: float,
        ds_river: str,
        ds_reach: str,
        ds_river_station: float,
    ):
        # create profile names
        depths, flows, wses = self.create_flow_depth_combinations(ds_depths, ds_wses, us_flows, min_depths)

        self.profile_names = [f"f_{int(flow)}-z_{str(depth).replace('.','_')}" for flow, depth in zip(flows, depths)]
        self.profile_count = len(self.profile_names)
        self.title = title

        self.contents = ""

        # write headers
        self.contents += self.write_headers(title, self.profile_names)

        # write discharges
        self.contents += self.write_discharges(flows, us_river, us_reach, us_river_station)

        # write DS boundary conditions
        self.contents += self.write_ds_known_ws(ds_wses, len(us_flows), ds_river, ds_reach, ds_river_station)

        # write flow file content
        self.write()

    def create_flow_depth_combinations(
        self, ds_depths: list, ds_wses: list, input_flows: np.array, min_depths: pd.Series
    ) -> tuple:
        """
        Create flow-depth-wse combinations

        Args:
            ds_depths (list): downstream depths
            ds_wses (list): downstream water surface elevations
            input_flows (np.array): Flows to create profiles names from. Combine with incremental depths
                of the downstream cross section of the reach
            min_depths (pd.Series): minimum depth to be included. (typically derived from a previous noraml depth run)

        Returns:
            tuple: tuple of profile_names, flows, and wses
        """

        depths, flows, wses = [], [], []

        for wse, depth in zip(ds_wses, ds_depths):

            for flow in input_flows:

                if depth > min_depths.loc[str(int(flow))]:

                    depths.append()
                    flows.append(flow)
                    wses.append(wse)

        return (depths, flows, wses)

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
        return "\n".join(lines)

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
            line += f"{str(int(flow)).rjust(8,' ')}"
            if (i + 1) % 10 == 0:
                lines.append(line)
                line = ""

        if (i + 1) % 10 != 0:
            lines.append(line)
        return "\n" + "\n".join(lines)

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

        return "\n" + "\n".join(lines)

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

        return "\n" + "\n".join(lines)


def clip_dem():
    pass


def create_terrain():
    pass


def read_rating_curves(self):
    pass


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
    for val in dict_of_ras_subclasses.values():
        extension_number.append(int(val.extension[2:]))

    return f"{(max(extension_number)+1):02d}"


class RasMap:
    """
    Represents a single RasMapper file.

    Attributes
    ----------
    text_file : Text file repressenting the RAS Mapper file
    contents : a text representation of the files contents
    version : HEC-RAS version for this RAS Mapper file

    """

    def __init__(self, path: str, version: str = 631):
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
            self.new_rasmap_content()

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

    def new_rasmap_content(self):
        """
        Populate the contents with boiler plate contents
        """
        if self.version in ["631", "6.31", "6.3.1", "63.1"]:
            self.contents = RASMAP_631
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

    def add_terrain(self, terrain_name: str):
        """
        Add Terrain to RasMap content
        """

        lines = []
        for line in self.contents.splitlines():

            if line == "  <Terrains />":

                lines.append(TERRAIN.replace(TERRAIN_NAME, terrain_name))
                continue

            lines.append(line)

        self.contents = "\n".join(lines)

    def write(self):
        """
        write Ras Map contents to file
        """

        print(f"writing: {self.text_file}")

        with open(self.text_file, "w") as f:
            f.write(self.contents)

        # write backup
        with open(self.text_file + ".backup", "w") as f:
            f.write(self.contents)


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
                        return projection
                else:
                    raise FileNotFoundError(f"Could not find projection file in {search_dir}")
