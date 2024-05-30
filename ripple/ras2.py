import os
from pathlib import Path
from typing import List

RAS_FILE_TYPES = ["Plan", "Flow", "Geometry", "Project"]

VALID_PLANS = [f".p{i:02d}" for i in range(1, 100)]
VALID_GEOMS = [f".g{i:02d}" for i in range(1, 100)]
VALID_STEADY_FLOWS = [f".f{i:02d}" for i in range(1, 100)]
VALID_UNSTEADY_FLOWS = [f".u{i:02d}" for i in range(1, 100)]
VALID_QUASISTEADY_FLOWS = [f".q{i:02d}" for i in range(1, 100)]


class RasManager:
    def __init__(self, ras_dir: str):
        self.__ras_dir = ras_dir
        self.__ras_files = self.__get_ras_files()

    def check_version_installed(self, version: str):
        pass

    def read_ras(self):
        pass

    def update_content(self):
        pass

    def set_current_plan(self):
        pass

    def write_to_new_file(self):
        pass

    def get_new_extension_number(self):
        pass

    def run_sim():
        pass


class RasTextFile:
    def __init__(self, ras_text_file_path):
        self.__ras_text_file_path = ras_text_file_path
        if not os.path.exists(ras_text_file_path):
            raise FileNotFoundError(f"could not find {ras_text_file_path}")
        else:
            self._ras_file_path = ras_text_file_path

    @property
    def contents(self):
        if not os.path.exists(self.__ras_text_file_path):
            raise FileNotFoundError(f"could not find {self.__ras_text_file_path}")
        with open(self.__ras_text_file_path) as f:
            return f.read().splitlines()

    @property
    def file_extension(self):
        return Path(self._ras_file_path).suffix

    def search_contents(self, search_string: str, token: str = "=", expect_one: bool = True):
        """
        Splits a line by a token and returns the second half of the line
            if the search_string is found in the first half
        """
        results = []
        for line in self.contents:
            if f"{search_string}{token}" in line:
                results.append(line.split(token)[1])

        if expect_one and len(results) > 1:
            raise ValueError(f"expected 1 result, got {len(results)}")
        elif expect_one and len(results) == 0:
            raise ValueError(f"expected 1 result, no results found")
        elif expect_one and len(results) == 1:
            return results[0]
        else:
            return results


class RasProject(RasTextFile):
    def __init__(self, ras_text_file_path: str):
        super().__init__(ras_text_file_path)
        if self.file_extension != ".prj":
            raise TypeError(f"Plan extenstion must be .prj, not {self.file_extension}")

    @property
    def title(self):
        return self.search_contents("Proj Title")

    @property
    def plans(self):
        return self.search_contents("Plan File", expect_one=False)

    @property
    def geoms(self):
        return self.search_contents("Geom File", expect_one=False)

    @property
    def unsteady_flows(self):
        return self.search_contents("Unsteady File", expect_one=False)

    @property
    def steady_flows(self):
        return self.search_contents("Flow File", expect_one=False)


class RasPlanText(RasTextFile):
    def __init__(self, ras_text_file_path: str):
        super().__init__(ras_text_file_path)
        if self.file_extension not in VALID_PLANS:
            raise TypeError(f"Plan extenstion must be one of .p01-.p99, not {self.file_extension}")

    @property
    def title(self):
        return self.search_contents("Plan Title")

    @property
    def version(self):
        return self.search_contents("Program Version")

    @property
    def plan_geom_file(self):
        return self.search_contents("Geom File")

    @property
    def plan_unsteady_flow(self):
        return self.search_contents("Unsteady File")

    @property
    def plan_steady_flow(self):
        return self.search_contents("Flow File")

    def parse_attrs(self):
        pass

    def new_plan(self):
        pass

    def populate_content(self):
        pass

    def write_new_plan(self):
        pass


class RasGeomText(RasTextFile):
    def __init__(self, ras_text_file_path: str):
        super().__init__(ras_text_file_path)
        if self.file_extension not in VALID_GEOMS:
            raise TypeError(f"Geometry extenstion must be one of .g01-.g99, not {self.file_extension}")

    @property
    def title(self):
        return self.search_contents("Geom Title")

    @property
    def version(self):
        return self.search_contents("Program Version")

    def determine_wse_increments_for_xs(self):
        pass

    def us_ds_most_xs(self):
        pass

    def scan_for_xs(self):
        pass

    def parse_reach_lengths(self):
        pass

    def parse_number_of_coords(self):
        pass

    def parse_coords(self):
        pass

    def parse_number_of_station_elevation_points(self):
        pass

    def parse_station_elevation_points(self):
        pass

    def parse_bank_stations(self):
        pass

    def xs_concave_hull(self):
        pass


class RasFlowText(RasTextFile):
    def __init__(self, ras_text_file_path: str):
        super().__init__(ras_text_file_path)
        if self.file_extension in VALID_UNSTEADY_FLOWS or self.file_extension in VALID_QUASISTEADY_FLOWS:
            raise NotImplementedError("only steady flow (f.**) supported")

        if self.file_extension not in VALID_STEADY_FLOWS:
            raise TypeError(f"Flow extenstion must be one of .f01-.f99, not {self.file_extension}")

    @property
    def title(self):
        return self.search_contents("Flow Title")

    @property
    def version(self):
        return self.search_contents("Program Version")

    @property
    def n_profiles(self):
        return int(self.search_contents("Number of Profiles"))

    def parse_attrs(self):
        pass

    def parse_flows(self):
        pass

    def max_flow_applied(self):
        pass

    def create_profile_names_kwseries(self):
        pass

    def write_headers(self):
        pass

    def write_discharges(self):
        pass

    def write_new_flow_rating_curves(self):
        pass

    def write_new_flow_production_runs(self):
        pass

    def write_ds_known_ws(self):
        pass

    def write_ds_normal_depth(self):
        pass

    def add_intermediate_known_wse(self):
        pass


def clip_dem():
    pass


def create_terrain():
    pass


def get_ras_projection(self):
    pass


def read_rating_curves(self):
    pass
