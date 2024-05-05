import json
from typing import Tuple

import geopandas as gpd
import pandas as pd
from fiona.errors import DriverError
from shapely.geometry import Point, MultiLineString
from shapely.ops import nearest_points


# Conflation constants
MIN_FLOW_FACTOR = 0.85
MAX_FLOW_FACTOR = 1.5

STAC_API_URL = "https://stac.dewberryanalytics.com"


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
        self._nwm_nodes = None
        self._ras_centerline = None
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
        self._nwm_branches = self.load_layer(self.nwm_gpkg, "branches")
        self._nwm_nodes = self.load_layer(self.nwm_gpkg, "nodes")
        self._ras_centerline = self.load_layer(self.ras_gpkg, "Rivers")
        self._ras_xs = self.load_layer(self.ras_gpkg, "XS")

    @property
    def ras_centerline(self) -> gpd.GeoDataFrame:
        if self.nwm_nodes.crs:
            return self._ras_centerline.to_crs(self.nwm_nodes.crs)
        else:
            raise AttributeError("nwm_nodes not found or do not have crs")

    @property
    def ras_xs(self) -> gpd.GeoDataFrame:
        if self.nwm_nodes.crs:
            return self._ras_xs.to_crs(self.nwm_nodes.crs)
        else:
            raise AttributeError("nwm_nodes not found or do not have crs")

    @property
    def ras_banks(self):
        raise NotImplementedError

    @property
    def nwm_nodes(self):
        return self._nwm_nodes

    @property
    def nwm_branches(self):
        return self._nwm_branches

    @property
    def ras_start_end_points(self) -> Tuple[Point, Point]:
        return endpoints_from_multiline(self.ras_centerline.geometry)

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

    def buffered_ras_centerline(self, buffer_distance: int = 10) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 10.

        Returns:
            gpd.GeoDataFrame: Buffered RAS centerline
        """
        if self.data_loaded:
            buffered_ras_centerline = self.ras_centerline.copy()
            buffered_ras_centerline.geometry = self.ras_centerline.geometry.buffer(
                buffer_distance
            )
            return buffered_ras_centerline

    def intersected_nodes_ras_river(
        self, buffer_distance: int = 100
    ) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 100.

        Returns:
            gpd.GeoDataFrame: NWM nodes intersecting the RAS river reach
        """
        if self.data_loaded:
            intersected_nodes_ras_river = gpd.sjoin(
                self.buffered_nodes(buffer_distance),
                self.ras_centerline,
                how="inner",
                predicate="intersects",
            )
            intersecting_branches = [
                int(v) for v in intersected_nodes_ras_river.branch_id.values
            ]
            return self.nwm_branches[
                self.nwm_branches["branch_id"].isin(intersecting_branches)
            ]

    def intersected_ras_river_nwm_branches(
        self, buffer_distance: int = 10
    ) -> gpd.GeoDataFrame:
        """
        Args:
            buffer_distance (int, optional): Buffer distance in meters. Defaults to 10.

        Returns:
            gpd.GeoDataFrame: NWM branches intersecting the RAS river reach
        """
        if self.data_loaded:
            intersected_ras_river_nwm_branches = gpd.sjoin(
                self.buffered_ras_centerline(buffer_distance),
                self.nwm_branches,
                how="inner",
                predicate="intersects",
            )
            intersecting_branches = [
                int(v) for v in intersected_ras_river_nwm_branches.branch_id.values
            ]
            return self.nwm_branches[
                self.nwm_branches["branch_id"].isin(intersecting_branches)
            ]


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

        model_data[data["branch_id"]]["downstream_data"] = {
            "node_id": data["control_by_node"]
        }
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


def endpoints_from_multiline(mline: MultiLineString) -> Tuple[Point, Point]:
    """
    Get the start and end points of a MultiLineString
    Args:
        An mline (MultiLineString) object


    Returns:
        (Point, Point) : Start and end points of the mline

    """
    merged_line = mline.unary_union
    start_point, end_point = merged_line.coords[0], merged_line.coords[-1]
    return Point(start_point), Point(end_point)


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

    if not "branch_id" in lines.columns:
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


def nwm_conflated_reaches(rfc: RasFimConflater, model_data: dict):
    """
    Args:
        rfc (RasFimConflater): RasFimConflater object
        model_data (dict): Dictionary of model data (conflation_summary output)

        Returns:
            gpd.GeoDataFrame: GeoDataFrame of conflated reaches
    """
    branches = [k for k in model_data.keys()]
    return rfc.nwm_branches[rfc.nwm_branches["branch_id"].isin(branches)].copy()


def find_ds_most_branch(
    rfc: RasFimConflater, control_nodes: gpd.GeoDataFrame, ras_stop_point: Point
):
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
    fb = FimBranch(
        rfc.nwm_branches.loc[
            rfc.nwm_branches["control_by_node"] == nearest_row.id.values[0]
        ]
    )
    return fb.branch_id


def walk_branches(
    rfc: RasFimConflater, us_most_branch_id: int, ds_most_branch_id: int
) -> dict:
    """
    Walk the branches from the upstream most branch to the downstream most branch

    The `if next_branch not in rfc.nwm_branches["branch_id"]` conditon
       seeks to adress cases where there are 2 downsteam most branches
       instead of one. This case occurs on a tributarty that ends at 
       a confluence and the main stem downstream and upstream branches
       are included in the  `candidate` branches.
    
    Args:

        rfc (RasFimConflater): RasFimConflater object
        us_most_branch_id (int): Upstream most branch id
        ds_most_branch_id (int): Downstream most branch id

    Returns:

        dict: Dictionary of branch information

    """
    start_branch = FimBranch(
        rfc.nwm_branches[rfc.nwm_branches["branch_id"] == us_most_branch_id]
    )

    end_branch = FimBranch(
        rfc.nwm_branches[rfc.nwm_branches["branch_id"] == ds_most_branch_id]
    )

    """
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

    """
    i = 0
    ras_fim_branches = {i: start_branch.to_dict()}
    # print(f"End info: branch={end_branch.branch_id}, control_by_node={end_branch.control_by_node}")
    fbranch = start_branch
    while fbranch.branch_id != ds_most_branch_id:
        i += 1
        next_branch = fbranch.control_by_node
        fbranch = FimBranch(
                rfc.nwm_branches[rfc.nwm_branches["branch_id"] == next_branch]
            )
        # print(f"Next info: branch={fbranch.branch_id}, control_by_node={fbranch.control_by_node}")
        if fbranch.control_by_node == end_branch.control_by_node:
            ras_fim_branches[i] = fbranch.to_dict()
            return ras_fim_branches
        elif next_branch not in rfc.nwm_branches["branch_id"]:
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


def find_flow_change_locations(branch_info_with_ras_detailed_xs_info):
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


def map_xs_data(rfc: RasFimConflater, xs_id: str):
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
