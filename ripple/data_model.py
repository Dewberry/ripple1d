import math
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from .utils import (
    data_pairs_from_text_block,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
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
                "ras_data": [self.ras_data],
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
        reach_lines = text_block_from_start_end_str(f"River Reach={river_reach}", "River Reach=", ras_data)[:-2]
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
        xs = []
        for header in self.reach_nodes:

            type, rs, left_reach_length, channel_reach_length, right_reach_length = header.split(",")

            if type != " 1 ":
                continue

            xs_lines = text_block_from_start_end_str(f"Type RM Length L Ch R ={header}", "Exp/Cntr", self.ras_data)
            xs.append(XS(xs_lines, self.river_reach, self.river, self.reach, self.projection))

        return xs

    @property
    def gdf(self):

        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                "number_of_coords": [self.number_of_coords],
                "coords": [self.coords],
                "ras_data": [self.ras_data],
            },
            crs=self.projection,
            geometry="geometry",
        )

    @property
    def xs_gdf(self):
        return pd.concat([xs.gdf for xs in self.cross_sections])
