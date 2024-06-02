import math
from dataclasses import dataclass
from typing import List

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

from .utils import (
    data_pairs_from_text_block,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
    text_block_from_start_str_to_empty_line,
)


@dataclass
class FlowChangeLocation:
    """
    HEC-RAS Flow Change Locations
    """

    river: str = None
    reach: str = None
    rs: float = None
    rs_str: str = None
    flows: list[float] = None


class XS:
    def __init__(self, ras_data: list, river_reach: str, river: str, reach: str, projection: str):

        self.ras_data = ras_data
        self.projection = projection
        self.river = river
        self.reach = reach
        self.river_reach = river_reach
        self.river_reach_rs = f"{river} {reach} {self.river_station}".rstrip("0").rstrip(".")

    def split_xs_header(self, position: int):
        """
        Example:
        Type RM Length L Ch R = 1 ,83554.  ,237.02,192.39,113.07
        """
        header = search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=True)

        return header.split(",")[position]

    @property
    def river_station(self):
        return float(self.split_xs_header(1))

    @property
    def left_reach_length(self):
        return float(self.split_xs_header(2))

    @property
    def channel_reach_length(self):
        return float(self.split_xs_header(3))

    @property
    def right_reach_length(self):
        return float(self.split_xs_header(4))

    @property
    def number_of_coords(self):
        return int(search_contents(self.ras_data, "XS GIS Cut Line", expect_one=True))

    @property
    def coords(self):
        lines = text_block_from_start_str_length(
            f"XS GIS Cut Line={self.number_of_coords}", math.ceil(self.number_of_coords / 2), self.ras_data
        )
        return data_pairs_from_text_block(lines, 32)

    @property
    def number_of_station_elevation_points(self):
        return int(search_contents(self.ras_data, "#Sta/Elev", expect_one=True))

    @property
    def station_elevation_points(self):
        lines = text_block_from_start_str_length(
            f"#Sta/Elev= {self.number_of_station_elevation_points} ",
            math.ceil(self.number_of_station_elevation_points / 5),
            self.ras_data,
        )
        return data_pairs_from_text_block(lines, 16)

    @property
    def bank_stations(self):
        return search_contents(self.ras_data, "Bank Sta", expect_one=True).split(",")

    @property
    def gdf(self):

        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                # "river_station": [self.river_station],
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
            crs=self.projection,
            geometry="geometry",
        )


class Reach:
    def __init__(self, ras_data: list, river_reach: str, projection: str):
        reach_lines = text_block_from_start_end_str(f"River Reach={river_reach}", "River Reach", ras_data)[:-1]
        self.ras_data = reach_lines
        self.projection = projection
        self.river_reach = river_reach
        self.river = river_reach.split(",")[0].rstrip()
        self.reach = river_reach.split(",")[1].rstrip()

        us_connection: str = None
        ds_connection: str = None

        us_cross_section: str = None
        ds_cross_section: str = None
        number_of_cross_sections: int = None

    @property
    def number_of_coords(self):
        return int(search_contents(self.ras_data, "Reach XY"))

    @property
    def coords(self):
        lines = text_block_from_start_str_length(
            f"Reach XY= {self.number_of_coords} ", math.ceil(self.number_of_coords / 2), self.ras_data
        )
        return data_pairs_from_text_block(lines, 32)

    @property
    def reach_nodes(self):
        return search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=False)

    @property
    def cross_sections(self):
        cross_sections = {}
        for header in self.reach_nodes:
            type, rs, left_reach_length, channel_reach_length, right_reach_length = header.split(",")
            if type != " 1 ":
                continue
            xs_lines = text_block_from_start_str_to_empty_line(f"Type RM Length L Ch R ={header}", self.ras_data)
            cross_section = XS(xs_lines, self.river_reach, self.river, self.reach, self.projection)
            cross_sections[cross_section.river_reach_rs] = cross_section

        return cross_sections

    @property
    def gdf(self):

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
            crs=self.projection,
            geometry="geometry",
        )

    @property
    def xs_gdf(self):
        return pd.concat([xs.gdf for xs in self.cross_sections.values()])


class Junction:

    @classmethod
    def from_text(cls, ras_data: List[str], junct: str, projection: str):
        inst = cls()
        inst.projeciton = projection
        inst.name = junct
        inst.ras_data = text_block_from_start_str_to_empty_line(f"Junct Name={junct}", ras_data)
        return inst

    @classmethod
    def from_gpkg(cls, gpkg_path: str):
        raise NotImplementedError
        # inst=cls()
        # gdf=gpd.read_file(gpkg_path,layer="Junction",driver="GPKG")
        # return inst

    def split_line(line: str, token: str, idx: int):
        return line.split(token)[idx]

    @property
    def x(self):
        return self.split_line(search_contents(self.ras_data, "Junct XY & Text X Y"), ",", 0)

    @property
    def y(self):
        return self.split_line(search_contents(self.ras_data, "Junct XY & Text X Y"), ",", 1)

    @property
    def point(self):
        return Point(self.x, self.y)

    @property
    def upstream_river_reaches(self):
        return search_contents(self.ras_data, "Up River,Reach", expect_one=False)

    @property
    def downstream_river_reaches(self):
        return search_contents(self.ras_data, "Dn River,Reach", expect_one=False)

    @property
    def junction_lengths(self):
        return search_contents(self.ras_data, "Junc L&A", expect_one=False)
