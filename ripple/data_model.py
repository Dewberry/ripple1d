"""Data model for shared utilities."""

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

from ripple.utils.ripple_utils import (
    data_pairs_from_text_block,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
    text_block_from_start_str_to_empty_line,
)


class RippleSourceModel:
    """Source Model structure for Ripple to create NwmReachModel's."""

    def __init__(self, model_directory: str):
        self.model_directory = model_directory
        self.model_name = Path(model_directory).name

    def derive_path(self, extension: str):
        """Derive path."""
        return str(Path(self.model_directory) / f"{self.model_name}{extension}")

    def file_exists(self, file_path: str) -> bool:
        """Check if file exists."""
        if os.path.exists(file_path):
            return True
        return False

    @property
    def ras_project_file(self):
        """RAS Project file."""
        return self.derive_path(".prj")

    @property
    def conflation_file(self):
        """Conflation file."""
        return self.derive_path(".conflation.json")

    @property
    def ras_gpkg_file(self):
        """RAS GeoPackage file."""
        return self.derive_path(".gpkg")

    @property
    def terrain_directory(self):
        """Terrain directory."""
        f"{self.model_directory}/Terrain"

    def nwm_conflation_parameters(self, nwm_id: str):
        """NWM Conflation parameters."""
        with open(self.conflation_file, "r") as f:
            conflation_parameters = json.loads(f.read())
        return conflation_parameters[nwm_id]


class NwmReachModel:
    """National Water Model reach-based HEC-RAS Model files and directory structure."""

    def __init__(self, model_directory: str):
        self.model_directory = model_directory
        self.model_name = Path(model_directory).name

    def derive_path(self, extension: str):
        """Derive path."""
        return str(Path(self.model_directory) / f"{self.model_name}{extension}")

    def file_exists(self, file_path: str) -> bool:
        """Check if file exists."""
        if os.path.exists(file_path):
            return True
        return False

    @property
    def ras_project_file(self):
        """RAS Project file."""
        return self.derive_path(".prj")

    @property
    def conflation_file(self):
        """Conflation file."""
        return self.derive_path(".ripple.json")

    @property
    def ras_gpkg_file(self):
        """RAS GeoPackage file."""
        return self.derive_path(".gpkg")

    @property
    def terrain_directory(self):
        """Terrain directory."""
        return str(Path(self.model_directory) / "Terrain")

    @property
    def ras_terrain_hdf(self):
        """RAS Terrain HDF file."""
        return str(Path(self.terrain_directory) / f"{self.model_name}.hdf")


@dataclass
class FlowChangeLocation:
    """HEC-RAS Flow Change Locations."""

    river: str = None
    reach: str = None
    rs: float = None
    flows: list[float] = None
    profile_names: list[str] = None


class XS:
    """HEC-RAS Cross Section."""

    def __init__(self, ras_data: list, river_reach: str, river: str, reach: str, crs: str):
        self.ras_data = ras_data
        self.crs = crs
        self.river = river
        self.reach = reach
        self.river_reach = river_reach
        self.river_reach_rs = f"{river} {reach} {self.river_station}"

    def split_xs_header(self, position: int):
        """
        Split cross section header.

        Example: Type RM Length L Ch R = 1 ,83554.  ,237.02,192.39,113.07.
        """
        header = search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=True)

        return header.split(",")[position]

    @property
    def river_station(self):
        """Cross section river station."""
        return float(self.split_xs_header(1))

    @property
    def left_reach_length(self):
        """Cross section left reach length."""
        return float(self.split_xs_header(2))

    @property
    def channel_reach_length(self):
        """Cross section channel reach length."""
        return float(self.split_xs_header(3))

    @property
    def right_reach_length(self):
        """Cross section right reach length."""
        return float(self.split_xs_header(4))

    @property
    def number_of_coords(self):
        """Number of coordinates in cross section."""
        try:
            return int(search_contents(self.ras_data, "XS GIS Cut Line", expect_one=True))
        except ValueError as e:
            return 0
            # raise NotGeoreferencedError(f"No coordinates found for cross section: {self.river_reach_rs} ")

    @property
    def thalweg(self):
        """Cross section thalweg elevation."""
        _, y = list(zip(*self.station_elevation_points))
        return min(y)

    @property
    def xs_max_elevation(self):
        """Cross section maximum elevation."""
        _, y = list(zip(*self.station_elevation_points))
        return max(y)

    @property
    def coords(self):
        """Cross section coordinates."""
        lines = text_block_from_start_str_length(
            f"XS GIS Cut Line={self.number_of_coords}",
            math.ceil(self.number_of_coords / 2),
            self.ras_data,
        )
        if lines:
            return data_pairs_from_text_block(lines, 32)

    @property
    def number_of_station_elevation_points(self):
        """Number of station elevation points."""
        return int(search_contents(self.ras_data, "#Sta/Elev", expect_one=True))

    @property
    def station_elevation_points(self):
        """Station elevation points."""
        lines = text_block_from_start_str_length(
            f"#Sta/Elev= {self.number_of_station_elevation_points} ",
            math.ceil(self.number_of_station_elevation_points / 5),
            self.ras_data,
        )
        return data_pairs_from_text_block(lines, 16)

    @property
    def bank_stations(self):
        """Bank stations."""
        return search_contents(self.ras_data, "Bank Sta", expect_one=True).split(",")

    @property
    def gdf(self):
        """Cross section geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                "river_station": [self.river_station],
                "river_reach_rs": [self.river_reach_rs],
                "thalweg": [self.thalweg],
                "xs_max_elevation": [self.xs_max_elevation],
                # "left_reach_length": [self.left_reach_length],
                # "right_reach_length": [self.right_reach_length],
                # "channel_reach_length": [self.channel_reach_length],
                "ras_data": ["\n".join(self.ras_data)],
                # "station_elevation_points": [self.station_elevation_points],
                # "bank_stations": [self.bank_stations],
                # "number_of_station_elevation_points": [self.number_of_station_elevation_points],
                # "number_of_coords": [self.number_of_coords],
                # "coords": [self.coords],
            },
            crs=self.crs,
            geometry="geometry",
        )


class Reach:
    """HEC-RAS River Reach."""

    def __init__(self, ras_data: list, river_reach: str, crs: str):
        reach_lines = text_block_from_start_end_str(f"River Reach={river_reach}", "River Reach", ras_data)
        self.ras_data = reach_lines
        self.crs = crs
        self.river_reach = river_reach
        self.river = river_reach.split(",")[0].rstrip()
        self.reach = river_reach.split(",")[1].rstrip()

        us_connection: str = None
        ds_connection: str = None

    @property
    def us_xs(self):
        """Upstream cross section."""
        return self.cross_sections[
            self.xs_gdf.loc[
                self.xs_gdf["river_station"] == self.xs_gdf["river_station"].max(),
                "river_reach_rs",
            ][0]
        ]

    @property
    def ds_xs(self):
        """Downstream cross section."""
        return self.cross_sections[
            self.xs_gdf.loc[
                self.xs_gdf["river_station"] == self.xs_gdf["river_station"].min(),
                "river_reach_rs",
            ][0]
        ]

    @property
    def number_of_cross_sections(self):
        """Number of cross sections."""
        return len(self.cross_sections)

    @property
    def number_of_coords(self):
        """Number of coordinates in reach."""
        return int(search_contents(self.ras_data, "Reach XY"))

    @property
    def coords(self):
        """Reach coordinates."""
        lines = text_block_from_start_str_length(
            f"Reach XY= {self.number_of_coords} ",
            math.ceil(self.number_of_coords / 2),
            self.ras_data,
        )
        return data_pairs_from_text_block(lines, 32)

    @property
    def reach_nodes(self):
        """Reach nodes."""
        return search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=False)

    @property
    def cross_sections(self):
        """Cross sections."""
        cross_sections = {}
        for header in self.reach_nodes:
            type, rs, left_reach_length, channel_reach_length, right_reach_length = header.split(",")[:5]
            if type != " 1 ":
                continue
            xs_lines = text_block_from_start_end_str(
                f"Type RM Length L Ch R ={header}",
                "Exp/Cntr=",
                self.ras_data,
                include_end_line=True,
            )
            cross_section = XS(xs_lines, self.river_reach, self.river, self.reach, self.crs)
            cross_sections[cross_section.river_reach_rs] = cross_section

        return cross_sections

    @property
    def gdf(self):
        """Reach geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                # "number_of_coords": [self.number_of_coords],
                # "coords": [self.coords],
                "ras_data": ["\n".join(self.ras_data)],
            },
            crs=self.crs,
            geometry="geometry",
        )

    @property
    def xs_gdf(self):
        """Cross section geodataframe."""
        return pd.concat([xs.gdf for xs in self.cross_sections.values()])


class Junction:
    """HEC-RAS Junction."""

    def __init__(self, ras_data: List[str], junct: str, crs: str):
        self.crs = crs
        self.name = junct
        self.ras_data = text_block_from_start_str_to_empty_line(f"Junct Name={junct}", ras_data)

    def split_lines(self, lines: str, token: str, idx: int):
        """Split lines."""
        return list(map(lambda line: line.split(token)[idx].rstrip(), lines))

    @property
    def x(self):
        """Junction x coordinate."""
        return self.split_lines([search_contents(self.ras_data, "Junct X Y & Text X Y")], ",", 0)

    @property
    def y(self):
        """Junction y coordinate."""
        return self.split_lines([search_contents(self.ras_data, "Junct X Y & Text X Y")], ",", 1)

    @property
    def point(self):
        """Junction point."""
        return Point(self.x, self.y)

    @property
    def upstream_rivers(self):
        """Upstream rivers."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Up River,Reach", expect_one=False),
                ",",
                0,
            )
        )

    @property
    def downstream_rivers(self):
        """Downstream rivers."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Dn River,Reach", expect_one=False),
                ",",
                0,
            )
        )

    @property
    def upstream_reaches(self):
        """Upstream reaches."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Up River,Reach", expect_one=False),
                ",",
                1,
            )
        )

    @property
    def downstream_reaches(self):
        """Downstream reaches."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Dn River,Reach", expect_one=False),
                ",",
                1,
            )
        )

    @property
    def junction_lengths(self):
        """Junction lengths."""
        return ",".join(self.split_lines(search_contents(self.ras_data, "Junc L&A", expect_one=False), ",", 0))

    @property
    def gdf(self):
        """Junction geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [self.point],
                "junction_lengths": [self.junction_lengths],
                "us_rivers": [self.upstream_rivers],
                "ds_rivers": [self.downstream_rivers],
                "us_reaches": [self.upstream_reaches],
                "ds_reaches": [self.downstream_reaches],
                "ras_data": ["\n".join(self.ras_data)],
            },
            geometry="geometry",
            crs=self.crs,
        )
