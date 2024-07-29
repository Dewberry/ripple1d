"""Data model for shared utilities."""

import glob
import json
import math
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import List

import geopandas as gpd
import pandas as pd
from shapely import offset_curve
from shapely.geometry import LineString, Point

from ripple.utils.ripple_utils import (
    data_pairs_from_text_block,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
    text_block_from_start_str_to_empty_line,
)
from ripple.utils.s3_utils import init_s3_resources, read_json_from_s3


class RasModelStructure:
    """Base Model structure for RAS models."""

    def __init__(self, model_directory: str):
        self.model_directory = model_directory
        self.model_basename = Path(model_directory).name

    @property
    def model_name(self):
        """Model name."""
        return self.model_basename.replace(".prj", "")

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
    def ras_gpkg_file(self):
        """RAS GeoPackage file."""
        return self.derive_path(".gpkg")

    @property
    def assets(self):
        """Model assets."""
        return glob.glob(f"{self.model_directory}/Terrain/*") + [
            f for f in glob.glob(f"{self.model_directory}/*") if not os.path.isdir(f)
        ]

    @property
    def terrain_assets(self):
        """Terrain assets."""
        return glob.glob(f"{self.model_directory}/Terrain/*")

    @property
    def thumbnail_png(self):
        """Thumbnail PNG."""
        return self.derive_path(".png")


class RippleSourceModel(RasModelStructure):
    """Source Model structure for Ripple to create NwmReachModel's."""

    def __init__(self, model_directory: str):
        super().__init__(model_directory)

    @property
    def conflation_file(self):
        """Conflation file."""
        return self.derive_path(".conflation.json")

    def nwm_conflation_parameters(self, nwm_id: str):
        """NWM Conflation parameters."""
        with open(self.conflation_file, "r") as f:
            conflation_parameters = json.loads(f.read())
        return conflation_parameters[nwm_id]


class NwmReachModel(RasModelStructure):
    """National Water Model reach-based HEC-RAS Model files and directory structure."""

    def __init__(self, model_directory: str):
        super().__init__(model_directory)

    @property
    def terrain_directory(self):
        """Terrain directory."""
        return str(Path(self.model_directory) / "Terrain")

    @property
    def ras_terrain_hdf(self):
        """RAS Terrain HDF file."""
        return str(Path(self.terrain_directory) / f"{self.model_name}.hdf")

    @property
    def fim_results_directory(self):
        """FIM results directory."""
        return str(Path(self.model_directory) / "fims")

    @property
    def fim_lib_assets(self):
        """Assets of the fim library."""
        return glob.glob(f"{self.fim_results_directory}/*/*")

    @property
    def fim_lib_stac_json_file(self):
        """FIM LIBRARY STAC JSON file."""
        return str(Path(self.fim_results_directory) / f"{self.model_name}.fim_lib.stac.json")

    @property
    def fim_results_database(self):
        """Results database."""
        return str(Path(self.fim_results_directory) / f"{self.model_name}.db")

    @property
    def fim_rating_curve(self):
        """FIM rating curve."""
        with sqlite3.connect(self.fim_results_database) as conn:
            cursor = conn.cursor()
            sql_query = f"""SELECT us_flow, us_wse, ds_wse
            FROM rating_curves
            WHERE reach_id={self.model_name}"""
            cursor.execute(sql_query)

            data = cursor.fetchall()
        data_list = ["Flow | US WSE | DS WSE"]
        for row in data:
            data_list.append(" | ".join([str(r) for r in row]))

        return data_list

    @property
    def crs(self):
        """Coordinate Reference System."""
        return self.ripple_parameters["crs"]

    def upload_files_to_s3(self, ras_s3_prefix: str, bucket: str):
        """Upload the model to s3."""
        _, client, _ = init_s3_resources()
        for file in self.assets:
            key = str(PurePosixPath(Path(file.replace(self.model_directory, ras_s3_prefix))))
            client.upload_file(
                Bucket=bucket,
                Key=key,
                Filename=file,
            )

    def upload_fim_lib_assets(self, s3_prefix: str, bucket: str):
        """Upload the fim lib to s3."""
        _, client, _ = init_s3_resources()
        for file in self.fim_lib_assets:
            parts = list(Path(file).parts[-2:])
            file_name = "-".join(parts)
            key = f"{s3_prefix}/{file_name}"

            client.upload_file(
                Bucket=bucket,
                Key=key,
                Filename=file,
            )
        file_name = os.path.basename(self.fim_lib_stac_json_file)
        client.upload_file(Bucket=bucket, Key=f"{s3_prefix}/{file_name}", Filename=self.fim_lib_stac_json_file)

    @property
    def ripple_parameters(self):
        """Ripple parameters."""
        with open(self.conflation_file, "r") as f:
            ripple_parameters = json.loads(f.read())
        return ripple_parameters

    def update_write_ripple_parameters(self, new_parameters: dict):
        """Write Ripple parameters."""
        parameters = self.ripple_parameters
        parameters.update(new_parameters)
        with open(self.conflation_file, "w") as f:
            f.write(json.dumps(parameters, indent=4))

    @property
    def conflation_file(self):
        """Conflation file."""
        return self.derive_path(".ripple.json")

    @property
    def model_stac_json_file(self):
        """STAC JSON file."""
        return self.derive_path(".model.stac.json")


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


class Structure:
    """Structure."""

    def __init__(self, ras_data: list, river_reach: str, river: str, reach: str, crs: str, us_xs: XS):
        self.ras_data = ras_data
        self.crs = crs
        self.river = river
        self.reach = reach
        self.river_reach = river_reach
        self.river_reach_rs = f"{river} {reach} {self.river_station}"
        self.us_xs = us_xs

    def split_structure_header(self, position: int):
        """
        Split Structure header.

        Example: Type RM Length L Ch R = 1 ,83554.  ,237.02,192.39,113.07.
        """
        header = search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=True)

        return header.split(",")[position]

    @property
    def river_station(self):
        """Structure river station."""
        return float(self.split_structure_header(1))

    def structure_data(self, position: int):
        """Structure data."""
        data = text_block_from_start_str_length(
            "Deck Dist Width WeirC Skew NumUp NumDn MinLoCord MaxHiCord MaxSubmerge Is_Ogee", 1, self.ras_data
        )
        return data[0].split(",")[position]

    @property
    def distance(self):
        """Distance to upstream cross section."""
        return float(self.structure_data(0))

    @property
    def width(self):
        """Structure width."""
        # TODO check units of the RAS model
        return float(self.structure_data(1))

    @property
    def gdf(self):
        """Structure geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.us_xs.coords).offset_curve(self.distance)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                "river_station": [self.river_station],
                "river_reach_rs": [self.river_reach_rs],
                "distance": [self.distance],
                "width": [self.width],
                "ras_data": ["\n".join(self.ras_data)],
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
            type, _, _, _, _ = header.split(",")[:5]
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
    def structures(self):
        """Structures."""
        structures = {}
        for header in self.reach_nodes:
            type, _, _, _, _ = header.split(",")[:5]
            if type == " 1 ":
                xs_lines = text_block_from_start_end_str(
                    f"Type RM Length L Ch R ={header}",
                    "Exp/Cntr=",
                    self.ras_data,
                    include_end_line=True,
                )
                cross_section = XS(xs_lines, self.river_reach, self.river, self.reach, self.crs)
                continue
            elif type == " 6 ":  # lateral structure
                continue
            elif type == " 5 ":  # inline structure
                continue
            structure_lines = text_block_from_start_end_str(
                f"Type RM Length L Ch R ={header}",
                "BC User HTab FreeFlow(D)=",
                self.ras_data,
                include_end_line=True,
            )
            structure = Structure(structure_lines, self.river_reach, self.river, self.reach, self.crs, cross_section)
            structures[structure.river_reach_rs] = structure

        return structures

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

    @property
    def structures_gdf(self):
        """Structures geodataframe."""
        return pd.concat([structure.gdf for structure in self.structures.values()])


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
