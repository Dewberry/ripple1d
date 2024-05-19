import json
from typing import Tuple, List

import geopandas as gpd
import pandas as pd
import numpy as np
from fiona.errors import DriverError
from shapely.geometry import Point, MultiLineString, LineString
from shapely.ops import nearest_points

# Conflation constants
MIN_FLOW_FACTOR = 0.85
MAX_FLOW_FACTOR = 1.5

STAC_API_URL = "https://stac2.dewberryanalytics.com"


def alt_river_reach_name(river_reach_name: str) -> str:
    """
    Replace " " with "-" in the Reach Name of a River Reach.
    Reach Names in XS data include spaces, while Reach Names in River data include dashes.
    This may be an artifact from MCAT-RAS, need to verify.
    """
    xs_river = river_reach_name.split(", ")[0]
    xs_reach = river_reach_name.split(", ")[1]
    return f'{xs_river}, {xs_reach.replace(" ", "-")}'


def buffer_and_intersect(points_gdf, lines_gdf, buffer_distance: int = 50) -> list:
    """
    Intersects ras river centerline points with nwm branches
    """
    points_gdf["geometry"] = points_gdf.geometry.buffer(buffer_distance)
    if points_gdf.crs != lines_gdf.crs:
        points_gdf = points_gdf.to_crs(lines_gdf.crs)
    join_gdf = gpd.sjoin(points_gdf, lines_gdf, predicate="intersects")
    return join_gdf["branch_id"].unique()


def convert_linestring_to_points(linestring: LineString, spacing=5) -> List[Point]:
    num_points = int(round(linestring.length / spacing))

    if num_points == 0:
        num_points = 1

    return [
        linestring.interpolate(distance)
        for distance in np.linspace(0, linestring.length, num_points)
    ]


def compare_nearest_points(candidate_branch_points, ras1) -> float:
    cb1 = gpd.GeoDataFrame(geometry=[Point(p) for p in candidate_branch_points])
    ras1 = gpd.GeoDataFrame(geometry=[Point(p) for p in ras1])
    multipoint = ras1.geometry.unary_union
    cb1["nearest_distance"] = cb1.geometry.apply(
        lambda point: point.distance(nearest_points(point, multipoint)[1])
    )
    return cb1["nearest_distance"].mean()


def count_intersecting_lines(ras1, cf1) -> int:
    if ras1.crs != cf1.crs:
        ras1 = ras1.to_crs(cf1.crs)
    join_gdf = gpd.sjoin(ras1, cf1, predicate="intersects")
    count = join_gdf.shape[0]
    return count


class FimBranch:
    def __init__(self, data):
        """
        Initialize a FimBranch object from a single row of a GeoDataFrame with the following columns:
            branch_id, control_by_node, reaches, control_nodes, flow_100_yr, flow_2_yr, geometry

        Args:
            data (dict): Dictionary of branch data

        """
        self.branch_id = int(list(data["branch_id"].values)[0])
        self.control_by_node = int(list(data["control_by_node"].values)[0])
        self.reaches = json.loads(list(data["reaches"].values)[0])
        self.control_nodes = json.loads(list(data["control_nodes"].values)[0])
        self.flow_100_yr = list(data["flow_100_yr"].values)[0]
        self.flow_2_yr = list(data["flow_2_yr"].values)[0]
        self.geometry = list(data["geometry"].values)[0]

    @classmethod
    def load_from_gdf(cls, gdf):
        data = gdf.to_dict()
        instance = cls(data)
        return instance

    def to_dict(self):
        return {
            "branch_id": int(self.branch_id),
            "control_by_node": int(self.control_by_node),
            "reaches": self.reaches,
            "control_nodes": self.control_nodes,
            "flow_100_yr": int(self.flow_100_yr),
            "flow_2_yr": int(self.flow_2_yr),
        }

    def to_geodataframe(self):
        data = {
            "branch_id": [self.branch_id],
            "control_by_node": [self.control_by_node],
            "reaches": [json.dumps(self.reaches)],
            "control_nodes": [json.dumps(self.control_nodes)],
            "flow_100_yr": [self.flow_100_yr],
            "flow_2_yr": [self.flow_2_yr],
            "geometry": [self.geometry],
        }
        return gpd.GeoDataFrame(pd.DataFrame(data), geometry="geometry")

    def __repr__(self):
        return f"FimBranch(branch_id={self.branch_id}, control_by_node={self.control_by_node})"


class RasFimConflater:
    """
    Conflate NWM and RAS data for a single river reach

    Args:
        nwm_gpkg (str): Path to the NWM GeoPackage
        ras_gpkg (str): Path to the RAS GeoPackage
        load_data (bool, optional): Load the data on initialization. Defaults to True.
        bucket (str, optional): S3 bucket to read data from. Defaults to "fim".

    Raises:
        ValueError: Required layer not found in the GeoPackage
        DriverError: Unable to read the GeoPackage
    """

    def __init__(
        self, nwm_gpkg: str, ras_gpkg: str, load_data: bool = True, bucket="fim"
    ):
        self.nwm_gpkg = nwm_gpkg
        self.ras_gpkg = ras_gpkg
        self.bucket = bucket
        self._nwm_branches = None
        self._ras_centerlines = None
        self._ras_xs = None
        self.data_loaded = False
        if load_data:
            self.load_data()
            self.data_loaded = True

    def __repr__(self):
        return f"Conflater(nwm_gpkg={self.nwm_gpkg}, ras_gpkg={self.ras_gpkg})"

    def load_layer(self, gpkg: str, layer: str):
        try:
            return gpd.read_file(gpkg, layer=layer)
        except Exception as e:
            if type(e) == ValueError:
                raise ValueError(f"Required layer '{layer}' not found in {gpkg}")
            elif type(e) == DriverError:
                raise DriverError(f"Unable to read {gpkg}")
            else:
                raise (e)

    def load_data(self):
        """
        Loads the NWM and RAS data from the GeoPackages
        """
        self._nwm_branches = self.load_layer(self.nwm_gpkg, "branches")
        self._ras_centerlines = self.load_layer(self.ras_gpkg, "Rivers")
        self._ras_xs = self.load_layer(self.ras_gpkg, "XS")

    @property
    def ras_centerlines(self) -> gpd.GeoDataFrame:
        if self._nwm_branches.crs:
            return self._ras_centerlines.to_crs(self._nwm_branches.crs)
        else:
            raise AttributeError(
                "nwm_branches required to convert crs for ras_centerlines"
            )

    @property
    def ras_river_reach_names(self) -> List[str]:
        return self.ras_centerlines.id.unique().tolist()

    @property
    def ras_xs(self) -> gpd.GeoDataFrame:
        if self._nwm_branches.crs:
            return self._ras_xs.to_crs(self._nwm_branches.crs)
        else:
            raise AttributeError("nwm_branches required to convert crs for ras_xs")

    @property
    def ras_banks(self):
        raise NotImplementedError

    @property
    def nwm_branches(self) -> gpd.GeoDataFrame:
        return self._nwm_branches

    def ras_centerline_by_river_reach_name(
        self, river_reach_name: str
    ) -> gpd.GeoDataFrame:
        if river_reach_name not in self.ras_river_reach_names:
            raise ValueError(
                f"'{river_reach_name}' not found in {self.ras_river_reach_names}"
            )
        else:
            return self.ras_centerlines[self.ras_centerlines["id"] == river_reach_name]

    def ras_centerline_densified_points(
        self, river_name_reach_name: str, densify_spacing: int = 5
    ) -> gpd.GeoDataFrame:
        """
        Converts ras centerline to points at densify_spacing=densify_spacing
        default units reflect the units of the centerline
        """
        ras_centerline = self.ras_centerline_by_river_reach_name(river_name_reach_name)
        assert (
            len(ras_centerline) == 1
        ), f"Multiple centerlines found for {river_name_reach_name}, this method expects a single LineString."
        num_points = int(round(ras_centerline.length.iloc[0] / densify_spacing))

        if num_points == 0:
            num_points = 1

        # return [
        #     ras_centerline.interpolate(distance)
        #     for distance in np.linspace(0, ras_centerline.length, num_points)
        # ]

        densified_points = [
            ras_centerline.interpolate(distance)
            for distance in np.linspace(0, ras_centerline.length, num_points)
        ]

        return gpd.GeoDataFrame(
            geometry=[Point(p[0]) for p in densified_points],
            crs=self.ras_centerlines.crs,
        )

    def xs_by_river_reach_name(self, river_reach_name: str) -> gpd.GeoDataFrame:
        """
        TODO: check mcat-ras for reach naming convention
        is the Reach 1 vs Reach-1 from RAS or MCAT-RAS?
        """
        if river_reach_name not in self.ras_river_reach_names:
            alt_river_reach = alt_river_reach_name(river_reach_name)
            if alt_river_reach not in self.ras_river_reach_names:
                raise ValueError(
                    f"'{river_reach_name}' or '{alt_river_reach}' not found in {self.ras_river_reach_names}"
                )
            else:
                river_reach_name = alt_river_reach

        matching_rows = []
        for _, row in self.ras_xs.iterrows():
            fields = json.loads(row.fields)
            if fields["RiverReachName"] == river_reach_name:
                matching_rows.append(row)

        return gpd.GeoDataFrame(
            matching_rows, columns=self.ras_centerlines.columns, crs=self.ras_xs.crs
        )

    def candidate_nwm_branches(
        self, ras_centerline_points: gpd.GeoDataFrame, buffer_distance: int = 10
    ) -> gpd.GeoDataFrame:
        branch_ids = buffer_and_intersect(
            ras_centerline_points, self.nwm_branches, buffer_distance
        )

        return self.nwm_branches.loc[self.nwm_branches.branch_id.isin(branch_ids)]
