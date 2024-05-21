import json
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from consts import MAX_FLOW_FACTOR, MIN_FLOW_FACTOR
from fiona.errors import DriverError
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge, nearest_points


# general geospatial functions
def endpoints_from_multiline(mline: MultiLineString) -> Tuple[Point, Point]:
    """
    Get the start and end points of a MultiLineString
    Args:
        An mline (MultiLineString) object


    Returns:
        (Point, Point) : Start and end points of the mline

    """
    merged_line = linemerge(mline)
    start_point, end_point = merged_line.coords[0], merged_line.coords[-1]
    return Point(start_point), Point(end_point)


def filter_gdf(gdf: gpd.GeoDataFrame, column_values: list, column_name: str = "id") -> gpd.GeoDataFrame:
    """
    Get subset of gdf when column_name is in the provided column_values
    """
    return gdf[~gdf[column_name].isin(column_values)]


def densify_points(gdf: gpd.GeoDataFrame, densify_spacing: int = 5, crs: pyproj.crs.crs.CRS = None) -> gpd.GeoDataFrame:
    """
    Converts linestring from gdf to points at densify_spacing=densify_spacing
    default units reflect the units of the ras_centerlines
    """

    assert gdf.shape[0] == 1, "Multiple centerlines found in gdf, this method expects a single LineString."
    num_points = int(round(gdf.geometry.length.iloc[0] / densify_spacing))

    if num_points == 0:
        num_points = 1

    densified_points = [
        gdf.geometry.interpolate(distance) for distance in np.linspace(0, gdf.geometry.length, num_points)
    ]

    return gpd.GeoDataFrame(
        geometry=[Point(p[0]) for p in densified_points],
        crs=gdf.crs if crs is None else crs,
    )


def buffer_points_and_intersect_line(
    points_gdf: gpd.GeoDataFrame,
    lines_gdf: gpd.GeoDataFrame,
    buffer_distance: int = 50,
) -> list:
    """
    Buffers the densified set of ras river centerline vertices and
    intersects with nwm branches
    """
    points_gdf["geometry"] = points_gdf.geometry.buffer(buffer_distance)
    if points_gdf.crs != lines_gdf.crs:
        points_gdf = points_gdf.to_crs(lines_gdf.crs)

    join_gdf = gpd.sjoin(points_gdf, lines_gdf, predicate="intersects")
    return join_gdf


def convert_linestring_to_points(
    linestring: LineString, crs: pyproj.crs.crs.CRS, point_spacing: int = 5
) -> gpd.GeoDataFrame:
    """
    Convert linestring to points with specified spacing along line

    """
    num_points = int(round(linestring.length / point_spacing))

    if num_points == 0:
        num_points = 1

    points = [linestring.interpolate(distance) for distance in np.linspace(0, linestring.length, num_points)]

    return gpd.GeoDataFrame(
        geometry=points,
        crs=crs,
    )


def cacl_avg_nearest_points(reference_gdf, compare_points_gdf) -> float:
    """
    Compute the average nearest point
    """
    multipoint = compare_points_gdf.geometry.unary_union
    reference_gdf["nearest_distance"] = reference_gdf.geometry.apply(
        lambda point: point.distance(nearest_points(point, multipoint)[1])
    )
    return reference_gdf["nearest_distance"].mean()


def count_intersecting_lines(ras_xs: gpd.GeoDataFrame, nwm_reaches: gpd.GeoDataFrame) -> int:
    """
    Determine how many lines intersect
    """
    if ras_xs.crs != nwm_reaches.crs:
        ras_xs = ras_xs.to_crs(nwm_reaches.crs)
    join_gdf = gpd.sjoin(ras_xs, nwm_reaches, predicate="intersects")
    return join_gdf


# RAS / FIM conflation functions
def alt_river_reach_name(river_reach_name: str) -> str:
    """
    Replace " " with "-" in the Reach Name of a River Reach.
    Reach Names in XS data include spaces, while Reach Names in River data include dashes.
    This may be an artifact from MCAT-RAS, need to verify.
    """
    xs_river = river_reach_name.split(", ")[0]
    xs_reach = river_reach_name.split(", ")[1]
    return f'{xs_river}, {xs_reach.replace(" ", "-")}'


def strip_river_reach_from_ras(river_reach_name: str) -> Tuple[str, str]:
    """
    Strip the river reach name from a RAS XS

    Args:
        river_reach_name (str): RAS XS RiverReachName

    Returns:
        Tuple: River and reach names
    """
    river = river_reach_name.split(",")[0]
    reach = river_reach_name.split(",")[1][1:]
    return river, reach


class FimBranch:
    def __init__(self, data: dict):
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
    def load_from_gdf(cls, gdf: gpd.GeoDataFrame):
        data = gdf.to_dict()
        instance = cls(data)
        return instance

    def to_dict(self) -> dict:
        return {
            "branch_id": int(self.branch_id),
            "control_by_node": int(self.control_by_node),
            "reaches": self.reaches,
            "control_nodes": self.control_nodes,
            "flow_100_yr": int(self.flow_100_yr),
            "flow_2_yr": int(self.flow_2_yr),
        }

    def to_geodataframe(self) -> gpd.GeoDataFrame:
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

    def __repr__(self) -> str:
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

    def __init__(self, nwm_gpkg: str, ras_gpkg: str, load_data: bool = True, bucket="fim"):
        self.nwm_gpkg = nwm_gpkg
        self.ras_gpkg = ras_gpkg
        self.bucket = bucket
        self._nwm_branches = None
        self._nwm_nodes = None
        self._ras_centerlines = None
        self._ras_xs = None
        self._common_crs = None
        self.data_loaded = False
        if load_data:
            self.load_data()
            self.data_loaded = True
            self._common_crs = self.nwm_branches.crs

    def __repr__(self) -> str:
        return f"RasFimConflater(nwm_gpkg={self.nwm_gpkg}, ras_gpkg={self.ras_gpkg})"

    def load_layer(self, gpkg: str, layer: str) -> gpd.GeoDataFrame:
        """
        Read layer from geopackage; return as geodataframe
        """
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
        self._nwm_nodes = self.load_layer(self.nwm_gpkg, "nodes")
        self._ras_centerlines = self.load_layer(self.ras_gpkg, "Rivers")
        self._ras_xs = self.load_layer(self.ras_gpkg, "XS")

    @property
    def ras_centerlines(self) -> gpd.GeoDataFrame:
        if self._nwm_branches.crs:
            return self._ras_centerlines.to_crs(self.common_crs)
        else:
            raise AttributeError("nwm_branches required to convert crs for ras_centerlines")

    @property
    def common_crs(self):
        return self._common_crs

    @property
    def ras_river_reach_names(self) -> List[str]:
        return self.ras_centerlines.id.unique().tolist()

    @property
    def ras_xs(self) -> gpd.GeoDataFrame:
        if self._nwm_branches.crs:
            return self._ras_xs.to_crs(self.common_crs)
        else:
            raise AttributeError("nwm_branches required to convert crs for ras_xs")

    @property
    def ras_banks(self):
        raise NotImplementedError

    @property
    def nwm_branches(self) -> gpd.GeoDataFrame:
        return self._nwm_branches

    @property
    def nwm_nodes(self):
        return self._nwm_nodes

    def ras_xs_by_river_reach_name(self, river_reach_name: str) -> gpd.GeoDataFrame:
        raise NotImplementedError

    def check_centerline(func):
        """
        Helper function to ensure that ras_centerline is available and that the centerline is specified
        If the centerline is not specified, the first centerline is used, with a check that there is only one centerline
        """

        def wrapper(self, *args, **kwargs):
            assert self.data_loaded, "Data not loaded"

            river_reach_name = kwargs.get("river_reach_name", None)
            if river_reach_name:
                print("No river_reach_name specified, using first and only centerline found.")
                centerline = self.ras_centerline_by_river_reach_name(river_reach_name)
            else:
                if self.ras_centerlines.shape[0] == 1:
                    centerline = self.ras_centerlines.geometry.iloc[0]
                else:
                    raise ValueError("Multiple centerlines found, please specify river_reach_name")
            kwargs["centerline"] = centerline
            return func(self, *args, **kwargs)

        return wrapper

    @check_centerline
    def ras_start_end_points(self, river_reach_name: str = None, centerline=None) -> Tuple[Point, Point]:
        """
        river_reach_name used by the decorator to get the centerline
        """
        return endpoints_from_multiline(centerline)

    @check_centerline
    def ras_centerline_by_river_reach_name(self, river_reach_name: str, centerline=None) -> gpd.GeoDataFrame:
        return self.ras_centerlines[self.ras_centerlines["id"] == river_reach_name]

    def ras_centerline_densified_points(
        self, river_name_reach_name: str = None, densify_spacing: int = 5
    ) -> gpd.GeoDataFrame:
        """
        Converts ras_centerlines to points at densify_spacing=densify_spacing
        default units reflect the units of the ras_centerlines
        """
        ras_centerline = self.ras_centerline_by_river_reach_name(river_name_reach_name)
        return densify_points(gdf=ras_centerline)

    def xs_by_river_reach_name(self, river_reach_name: str = None) -> gpd.GeoDataFrame:
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

        return gpd.GeoDataFrame(matching_rows, columns=self.ras_centerlines.columns, crs=self.common_crs)

    def candidate_nwm_branches_via_point_buffer(
        self, ras_centerline_points: gpd.GeoDataFrame, buffer_distance: int = 10
    ) -> gpd.GeoDataFrame:
        """
        Find candidate NWM branches that intersect the buffered ras centerline points
        buffer_distance uses nwm crs units
        """
        results = buffer_points_and_intersect_line(ras_centerline_points, self.nwm_branches, buffer_distance)

        return self.nwm_branches.loc[self.nwm_branches.branch_id.isin(results["branch_id"].values)]

    def buffered_nodes(self, buffer_distance: int = 100) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 100.

        Returns:
            gpd.GeoDataFrame: Buffered NWM nodes
        """
        if self.data_loaded:
            buffered_nodes = self.nwm_nodes.copy()
            buffered_nodes.geometry = self.nwm_nodes.geometry.buffer(buffer_distance)
            return buffered_nodes

    @check_centerline
    def buffered_ras_centerline(
        self, river_reach_name: str = None, buffer_distance: int = 10, centerline=None
    ) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 10.

        Returns:
            gpd.GeoDataFrame: Buffered RAS centerline
        """
        return centerline.buffer(buffer_distance)

    @check_centerline
    def intersected_nodes_ras_river(
        self, river_reach_name: str = None, buffer_distance: int = 100, centerline=None
    ) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 100.

        Returns:
            gpd.GeoDataFrame: NWM nodes intersecting the RAS river reach
        """

        intersected_nodes_ras_river = gpd.sjoin(
            self.buffered_nodes(buffer_distance),
            centerline,
            how="inner",
            predicate="intersects",
        )
        intersecting_branches = [int(v) for v in intersected_nodes_ras_river.branch_id.values]
        return self.nwm_branches[self.nwm_branches["branch_id"].isin(intersecting_branches)]

    @check_centerline
    def intersected_ras_river_nwm_branches(
        self, river_reach_name: str = None, buffer_distance: int = 10, centerline=None
    ) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 10.

        Returns:
            gpd.GeoDataFrame: NWM branches intersecting the RAS river reach
        """
        intersected_ras_river_nwm_branches = gpd.sjoin(
            gpd.GeoDataFrame(
                geometry=[self.buffered_ras_centerline(buffer_distance=buffer_distance)],
                crs=self.common_crs,
            ),
            self.nwm_branches,
            how="inner",
            predicate="intersects",
        )
        intersecting_branches = [int(v) for v in intersected_ras_river_nwm_branches.branch_id.values]
        return self.nwm_branches[self.nwm_branches["branch_id"].isin(intersecting_branches)]


def conflation_summary(rfc: RasFimConflater, branch_info_with_ras_xs, flow_changes):
    model_data = {}
    for key in branch_info_with_ras_xs.keys():
        data = branch_info_with_ras_xs[key]
        branch_flow_changes = [d for d in data["control_nodes"] if d in flow_changes]
        model_data[data["branch_id"]] = {
            "flows": {
                "flow_100_yr_plus": data["flow_100_yr"] * MAX_FLOW_FACTOR,
                "flow_2_yr_minus": data["flow_2_yr"] * MIN_FLOW_FACTOR,
            },
            "intermediate_data": [],
        }

        model_data[data["branch_id"]]["upstream_data"] = {"node_id": data["branch_id"]}

        model_data[data["branch_id"]]["upstream_data"].update(
            **map_xs_data(rfc, nwm_node_ras_xs_mapping(rfc, data["branch_id"]))
        )

        model_data[data["branch_id"]]["downstream_data"] = {"node_id": data["control_by_node"]}
        model_data[data["branch_id"]]["downstream_data"].update(
            **map_xs_data(
                rfc,
                nwm_node_ras_xs_mapping(rfc, data["control_by_node"]),
            ),
        )

        for node_id in data["control_nodes"]:
            if node_id == data["branch_id"]:
                # print("skipping upstream")
                continue

            elif node_id in branch_flow_changes:
                ras_info = map_xs_data(
                    rfc,
                    nwm_node_ras_xs_mapping(rfc, node_id),
                )
                ras_info["node_id"] = node_id
                model_data[data["branch_id"]]["intermediate_data"].append(ras_info)

    return model_data


def nearest_line_to_point(lines: gpd.GeoDataFrame, point: Point) -> int:
    """
    Find the line segment nearest the point in a GeoDataFrame of line segments
        point. (e.g. find the nearest reach in a subset of NWM reaches the start point of a RAS river reach and a)

    Args:
        lines (gpd.GeoDataFrame): GeoDataFrame of line segments
        point (Point): Point to find the nearest line to

    Returns:
        int: Value of the column of the nearest line segment
    """

    if "branch_id" not in lines.columns:
        raise ValueError("required `branch_id` column not found in GeoDataFrame")

    start_branch_distance = 1e9
    for row in lines.itertuples():
        start_distance = row.geometry.distance(point)
        if start_distance < start_branch_distance:
            start_branch_distance = start_distance
            start_branch_id = row.branch_id

    if start_branch_distance > 1e8:
        raise ValueError(
            f"Unable to associate branch with point, minimum distance for {start_branch_id} : {start_branch_distance}"
        )
    return start_branch_id


def nwm_conflated_reaches(rfc: RasFimConflater, model_data: dict) -> RasFimConflater:
    """
    Args:
        rfc (RasFimConflater): RasFimConflater object
        model_data (dict): Dictionary of model data (conflation_summary output)

        Returns:
            gpd.GeoDataFrame: GeoDataFrame of conflated reaches
    """
    branches = [k for k in model_data.keys()]
    return rfc.nwm_branches[rfc.nwm_branches["branch_id"].isin(branches)].copy()


def find_ds_most_branch(rfc: RasFimConflater, control_nodes: gpd.GeoDataFrame, ras_stop_point: Point) -> FimBranch:
    """
    Find the downstream most branch from a point

    Args:
        rfc (RasFimConflater): RasFimConflater object
        control_nodes (gpd.GeoDataFrame): GeoDataFrame of control nodes
        ras_stop_point (Point): Point to find the nearest control node to

    Returns:
        int: Downstream most branch id
    """
    nearest_geom = nearest_points(ras_stop_point, control_nodes.unary_union)[1]
    nearest_row = control_nodes[control_nodes.geometry == nearest_geom]
    fb = FimBranch(rfc.nwm_branches.loc[rfc.nwm_branches["control_by_node"] == nearest_row.id.values[0]])
    return fb.branch_id


def walk_branches(rfc: RasFimConflater, us_most_branch_id: int, ds_most_branch_id: int) -> dict:
    """
    Walk the branches from the upstream most branch to the downstream most branch

    The `if next_branch not in rfc.nwm_branches["branch_id"]` conditon
       seeks to adress cases where there are 2 downsteam most branches
       instead of one. This case occurs on a tributarty that ends at 
       a confluence and the main stem downstream and upstream branches
       are included in the  `candidate` branches.

    The ds branch is identified by checking the ras end point with the stream
    network using a near point search. The near point search returns the nearest
    branch which can be us or downstream. We want the downstream branch. The
    upstream branch can sometimes be picked up instead. The following conditions
    checks for that and corrects it.

        us main branch     \\    /  trib
                            \\  /
        rasriver endpoint  . \\/
                              \\
                               \\
        ds main branch          \\
        
    
    Args:

        rfc (RasFimConflater): RasFimConflater object
        us_most_branch_id (int): Upstream most branch id
        ds_most_branch_id (int): Downstream most branch id

    Returns:

        dict: Dictionary of branch information

    """
    start_branch = FimBranch(rfc.nwm_branches[rfc.nwm_branches["branch_id"] == us_most_branch_id])

    end_branch = FimBranch(rfc.nwm_branches[rfc.nwm_branches["branch_id"] == ds_most_branch_id])

    i = 0
    ras_fim_branches = {i: start_branch.to_dict()}

    if start_branch.control_by_node == end_branch.control_by_node:
        return ras_fim_branches

    fbranch = start_branch
    while fbranch.branch_id != ds_most_branch_id:
        i += 1
        next_branch = fbranch.control_by_node
        # print(next_branch)
        try:
            fbranch = FimBranch(rfc.nwm_branches[rfc.nwm_branches["branch_id"] == next_branch])
        except IndexError:
            return ras_fim_branches

        if fbranch.control_by_node == end_branch.control_by_node:
            ras_fim_branches[i] = fbranch.to_dict()
            return ras_fim_branches

        else:
            ras_fim_branches[i] = fbranch.to_dict()

    return ras_fim_branches


def nwm_node_ras_xs_mapping(rfc: RasFimConflater, node_id: int) -> int:
    """
    Map NWM node to RAS XS

    Args:
        rfc (RasFimConflater): RasFimConflater object
        node_id (int): NWM node id

    Returns:
        int: RAS XS id
    """
    node = rfc.nwm_nodes[rfc.nwm_nodes["node_id"] == node_id]
    distances = rfc.ras_xs.distance(node.geometry.values[0])
    nearest_xs_index = distances.idxmin()
    nearest_xs_id = rfc.ras_xs.loc[nearest_xs_index].id
    return nearest_xs_id


def map_control_nodes_to_xs(rfc, branch_info) -> dict:
    """
    Map control nodes to RAS XS

    Args:
        rfc (RasFimConflater): RasFimConflater object
        branch_info (dict): Dictionary of branch information

    Returns:
        dict: Dictionary of branch information with RAS XS information
    """
    for k, v in branch_info.items():
        control_nodes = {}
        for node in v["control_nodes"]:
            ras_xs_id = nwm_node_ras_xs_mapping(rfc, node)
            control_nodes[node] = ras_xs_id
            branch_info[k]["ras_xs_at_control"] = control_nodes
    return branch_info


def ras_xs_geometry_data(rfc: RasFimConflater, xs_id: str) -> Tuple[int, int]:
    """
    Get the min and max elevation of a RAS XS

    Args:
        rfc (RasFimConflater): RasFimConflater object
        xs_id (int): RAS XS id

    Returns:
        Tuple: Min and max elevation of the RAS XS
    """
    xs = rfc.ras_xs[rfc.ras_xs["id"] == xs_id]
    multiline = xs.geometry.values[0]
    if isinstance(multiline, MultiLineString):
        for linestring in multiline.geoms:
            min_el = min([p[2] for p in linestring.coords])
            max_el = max([p[2] for p in linestring.coords])
    return min_el, max_el


def find_flow_change_locations(branch_info_with_ras_detailed_xs_info) -> list:
    """
    Find flow change locations

    Args:
        branch_info_with_ras_detailed_xs_info (dict): Dictionary of branch information with RAS XS information

    Returns:
        list: List of flow change locations
    """
    flow_changes = []
    for idx in branch_info_with_ras_detailed_xs_info.keys():
        data = branch_info_with_ras_detailed_xs_info[idx]
        if idx == 0:
            continue
        else:
            flow_changes.extend(list(data["ras_xs_at_control"].keys()))
    return flow_changes


def map_xs_data(rfc: RasFimConflater, xs_id: str) -> dict:
    """
    Map XS data

    Args:
        rfc (RasFimConflater): RasFimConflater object
        xs_id (int): RAS XS id

    Returns:
        dict: Dictionary of XS data
    """
    dfslice = rfc.ras_xs[rfc.ras_xs["id"] == xs_id]
    river_reach_name = json.loads(dfslice.fields.values[0])["RiverReachName"]
    river, reach = strip_river_reach_from_ras(river_reach_name)
    min_el, max_el = ras_xs_geometry_data(rfc, xs_id)
    return {
        "xs_id": xs_id,
        "river": river,
        "reach": reach,
        "min_elevation": min_el,
        "max_elevation": max_el,
    }


def calculate_conflation_metrics(
    rfc: RasFimConflater,
    candidate_branches: gpd.GeoDataFrame,
    xs_group: gpd.GeoDataFrame,
    ras_points: gpd.GeoDataFrame,
) -> dict:
    next_round_candidates = []
    xs_hits_ids = []
    total_hits = 0
    for i in candidate_branches.index:
        candidate_branch_points = convert_linestring_to_points(candidate_branches.loc[i].geometry, crs=rfc.common_crs)
        if cacl_avg_nearest_points(candidate_branch_points, ras_points) < 2000:
            next_round_candidates.append(candidate_branches.loc[i]["branch_id"])
            gdftmp = gpd.GeoDataFrame(geometry=[candidate_branches.loc[i].geometry], crs=rfc.nwm_branches.crs)
            xs_hits = count_intersecting_lines(xs_group, gdftmp)

            total_hits += xs_hits.shape[0]
            xs_hits_ids.extend(xs_hits.id.tolist())

            # print(total_hits, xs_group.shape[0])

    dangling_xs = filter_gdf(xs_group, xs_hits_ids)

    dangling_xs_interesects = gpd.sjoin(dangling_xs, rfc.nwm_branches, predicate="intersects")

    conflation_score = round(total_hits / xs_group.shape[0], 2)

    if conflation_score == 1:
        conlfation_notes = "Probable Conflation, no dangling xs"
        manual_check_required = False

    elif dangling_xs_interesects.shape[0] == 0:
        conlfation_notes = f"Probable Conflation. Score = {conflation_score}% with {dangling_xs.shape[0]} dangling xs"
        manual_check_required = False

    elif conflation_score >= 0.25:
        conlfation_notes = "Possible Conflation: partial nwm branch coverage"
        manual_check_required = True

    elif conflation_score < 0.25:
        conlfation_notes = "Unable to conflate: potential disconnected branches"
        manual_check_required = True

    elif conflation_score > 1:
        conlfation_notes = "Unable to conflate: potential diverging branches"
        manual_check_required = True

    else:
        conlfation_notes = "Unknown error"
        manual_check_required = True

    # Convert next_round_candidates from int64 to serialize
    conlfation_metrics = {
        "fim_branches": [int(c) for c in next_round_candidates],
        "conflation_score": round(total_hits / xs_group.shape[0], 2),
        "conlfation_notes": conlfation_notes,
        "manual_check_required": manual_check_required,
    }
    return conlfation_metrics
