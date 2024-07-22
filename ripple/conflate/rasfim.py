"""Conflation classes and functions."""

import json
import logging
from collections import OrderedDict
from typing import List, Tuple

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from fiona.errors import DriverError
from shapely.geometry import LineString, MultiLineString, Point, Polygon, box
from shapely.ops import linemerge, nearest_points, transform

HIGH_FLOW_FACTOR = 1.2

NWM_CRS = """PROJCRS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",
    BASEGEOGCRS["NAD83",DATUM["North American Datum 1983",
    ELLIPSOID["GRS 1980",6378137,298.257222101004,LENGTHUNIT["metre",1]]],
    PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],
    ID["EPSG",4269]],CONVERSION["unnamed",METHOD["Albers Equal Area",
    ID["EPSG",9822]],PARAMETER["Latitude of 1st standard parallel"
    ,29.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]]
    ,PARAMETER["Latitude of 2nd standard parallel",45.5,
    ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],
    PARAMETER["Latitude of false origin",23,ANGLEUNIT["degree",0.0174532925199433],
    ID["EPSG",8821]],PARAMETER["Longitude of false origin",-96,
    ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],
    PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],
    ID["EPSG",8826]],PARAMETER["Northing at false origin",0,
    LENGTHUNIT["metre",1],ID["EPSG",8827]]],CS[Cartesian,2],
    AXIS["easting",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],
    AXIS["northing",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]"""


def ensure_geometry_column(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Confirm that there exists a geometry column in the GeoDataFrame."""
    if "geom" in gdf.columns:
        return gdf
    elif "geometry" in gdf.columns:
        gdf.rename(columns={"geometry": "geom"}, inplace=True)
        gdf.set_geometry("geom")
        return gdf
    else:
        raise KeyError(f"Expecting a `geom` or `geometry` column, did not find in {gdf.columns}")


class RasFimConflater:
    """
    Conflate NWM and RAS data for a single river reach.

    Args:
        nwm_parquet (str): Path to the NWM Parquet file converted to parquet from:
            s3://noaa-nws-owp-fim/rasfim/inputs/X-National_Datasets/nwm_flows.gpkg
        ras_gpkg (str): Path to the RAS GeoPackage
        load_data (bool, optional): Load the data on initialization. Defaults to True.
        bucket (str, optional): S3 bucket to read data from. Defaults to "fim".

    Raises
    ------
        ValueError: Required layer not found in the GeoPackage
        DriverError: Unable to read the GeoPackage
    """

    def __init__(self, nwm_pq: str, ras_gpkg: str, load_data: bool = True, bucket="fim"):
        self.nwm_pq = nwm_pq
        self.ras_gpkg = ras_gpkg
        self.bucket = bucket
        self._nwm_reaches = None

        self._ras_centerlines = None
        self._ras_xs = None
        self._ras_junctions = None
        self._common_crs = None
        self.__data_loaded = False
        if load_data:
            self.load_data()
            self.__data_loaded = True
            self._common_crs = NWM_CRS

    def __repr__(self):
        """Return the string representation of the object."""
        return f"RasFimConflater(nwm_pq={self.nwm_pq}, ras_gpkg={self.ras_gpkg})"

    def set_ras_gpkg(self, ras_gpkg, load_data=True):
        """Set the RAS GeoPackage and optionally load the data."""
        self.ras_gpkg = ras_gpkg
        if load_data:
            self.load_gpkg(self.ras_gpkg)

    def load_gpkg(self, gpkg: str):
        """Load the RAS data from the GeoPackage."""
        layers = fiona.listlayers(gpkg)
        if "River" in layers:
            self._ras_centerlines = gpd.read_file(self.ras_gpkg, layer="River")
        if "XS" in layers:
            self._ras_xs = gpd.read_file(self.ras_gpkg, layer="XS")
        if "Junction" in layers:
            self._ras_junctions = gpd.read_file(self.ras_gpkg, layer="Junction")

    def load_pq(self, nwm_pq: str):
        """Load the NWM data from the Parquet file."""
        try:
            nwm_reaches = gpd.read_parquet(nwm_pq)
            nwm_reaches = nwm_reaches.rename(columns={"geom": "geometry"})
            self._nwm_reaches = nwm_reaches.set_geometry("geometry")
        except Exception as e:
            if type(e) == DriverError:
                raise DriverError(f"Unable to read {nwm_pq}")
            else:
                raise (e)

    def load_data(self):
        """Load the NWM and RAS data from the GeoPackages."""
        self.load_pq(self.nwm_pq)
        self.load_gpkg(self.ras_gpkg)

    def ensure_data_loaded(func):
        """Ensure that the data is loaded before accessing the properties Decorator."""

        def wrapper(self, *args, **kwargs):
            if not self.__data_loaded:
                raise Exception("Data not loaded")
            return func(self, *args, **kwargs)

        return wrapper

    @property
    def common_crs(self):
        """Return the common CRS for the NWM and RAS data."""
        return self._common_crs

    @property
    @ensure_data_loaded
    def ras_centerlines(self) -> gpd.GeoDataFrame:
        """RAS centerlines."""
        return self._ras_centerlines.to_crs(self.common_crs)

    @property
    @ensure_data_loaded
    def ras_river_reach_names(self) -> List[str]:
        """Return the unique river reach names in the RAS data."""
        return self.ras_centerlines["river_reach"].unique().tolist()

    @property
    @ensure_data_loaded
    def ras_xs(self) -> gpd.GeoDataFrame:
        """RAS cross sections."""
        return self._ras_xs.to_crs(self.common_crs)

    @property
    @ensure_data_loaded
    def ras_junctions(self) -> gpd.GeoDataFrame:
        """RAS junctions."""
        try:
            return self._ras_junctions.to_crs(self.common_crs)
        except ValueError:
            return None

    @property
    def ras_xs_bbox(self) -> Polygon:
        """Return the bounding box for the RAS cross sections."""
        return self.get_projected_bbox(self.ras_xs)

    @property
    def ras_banks(self):
        """Return the banks of the RAS cross sections."""
        raise NotImplementedError

    def get_projected_bbox(self, gdf: gpd.GeoDataFrame) -> Polygon:
        """Return the bounding box for the GeoDataFrame in the common CRS."""
        assert gdf.crs, "GeoDataFrame must have a CRS"
        project = pyproj.Transformer.from_crs(gdf.crs, NWM_CRS, always_xy=True).transform
        return transform(project, box(*tuple(gdf.total_bounds)))

    @property
    @ensure_data_loaded
    def nwm_reaches(self) -> gpd.GeoDataFrame:
        """NWM reaches."""
        return self._nwm_reaches

    @property
    def local_nwm_reaches(self) -> gpd.GeoDataFrame:
        """NWM reaches that intersect the RAS cross sections."""
        return self.nwm_reaches[self.nwm_reaches.intersects(self.ras_xs_bbox)]

    @property
    def local_gages(self) -> dict:
        """Local gages for the NWM reaches."""
        local_reaches = self.local_nwm_reaches
        gages = local_reaches["gages"]
        reach_ids = local_reaches["ID"]
        return {reach_id: gage for reach_id, gage in zip(reach_ids, gages) if pd.notna(gage) and gage.strip()}

    def local_lakes(self):
        """Local lakes for the NWM reaches."""
        raise NotImplementedError

    def check_centerline(func):
        """
        Ensure that ras_centerline is available and that the centerline is specified.

        Helper function.

        If the centerline is not specified, the first centerline is used, with a check that there is only one centerline.
        """

        def wrapper(self, *args, **kwargs):
            assert self.__data_loaded, "Data not loaded"

            river_reach_name = kwargs.get("river_reach_name", None)

            if river_reach_name:
                logging.debug(f"check_centerline river_reach_name: {river_reach_name}")
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
        """River_reach_name used by the decorator to get the centerline."""
        if river_reach_name:
            centerline = self.ras_centerline_by_river_reach_name(river_reach_name)
        return endpoints_from_multiline(centerline)

    def ras_centerline_by_river_reach_name(self, river_reach_name: str) -> LineString:
        """Return the centerline for the specified river reach."""
        return self.ras_centerlines[self.ras_centerlines["river_reach"] == river_reach_name].geometry.iloc[0]

    def xs_by_river_reach_name(self, river_reach_name: str) -> gpd.GeoDataFrame:
        """Return the cross sections for the specified river reach."""
        return self.ras_xs[self.ras_xs["river_reach"] == river_reach_name]

    def river_reach_name_by_xs(self, xs_id: str) -> Tuple[str, str]:
        """Return the river and reach names for the specified cross section."""
        data = self.ras_xs[self.ras_xs["ID"] == int(xs_id)]
        if data.empty:
            raise ValueError(f"XS ID {xs_id} not found in ras_xs")
        elif data.shape[0] != 1:
            raise ValueError(f"Multiple XS found with ID = {xs_id}")
        else:
            return data.iloc[0]["river"], data.iloc[0]["reach"]


# general geospatial functions
def endpoints_from_multiline(mline: MultiLineString) -> Tuple[Point, Point]:
    """Return the start and end points of a MultiLineString."""
    if isinstance(mline, MultiLineString):
        # TODO: Ensure the centerlines in the GPKG are always the same type (i.e. MultiLineString)
        merged_line = linemerge(mline)
    else:
        merged_line = mline
    start_point, end_point = merged_line.coords[0], merged_line.coords[-1]
    return Point(start_point), Point(end_point)


def nearest_line_to_point(lines: gpd.GeoDataFrame, point: Point, column_id: str = "ID") -> int:
    """Return the ID of the line closest to the point."""
    if not column_id in lines.columns:
        raise ValueError(f"required `ID` column not found in GeoDataFrame with columns: {lines.columns}")

    start_reach_distance = 1e9
    start_reach_id = None
    for _, row in lines.iterrows():
        start_distance = row["geometry"].distance(point)
        if start_distance < start_reach_distance:
            start_reach_distance = start_distance
            start_reach_id = row[column_id]

    if start_reach_distance > 1e8:
        raise ValueError(
            f"Unable to associate reach with point, minimum distance for {start_reach_id} : {start_reach_distance}"
        )
    return start_reach_id


def convert_linestring_to_points(
    linestring: LineString, crs: pyproj.crs.crs.CRS, point_spacing: int = 5
) -> gpd.GeoDataFrame:
    """Convert a LineString to a GeoDataFrame of Points."""
    num_points = int(round(linestring.length / point_spacing))

    if num_points == 0:
        num_points = 1

    points = [linestring.interpolate(distance) for distance in np.linspace(0, linestring.length, num_points)]

    return gpd.GeoDataFrame(
        geometry=points,
        crs=crs,
    )


def cacl_avg_nearest_points(reference_gdf: gpd.GeoDataFrame, compare_points_gdf: gpd.GeoDataFrame) -> float:
    """Calculate the average distance between the reference points and the nearest points in the comparison GeoDataFrame."""
    multipoint = compare_points_gdf.geometry.union_all()
    reference_gdf["nearest_distance"] = reference_gdf.geometry.apply(
        lambda point: point.distance(nearest_points(point, multipoint)[1])
    )
    return reference_gdf["nearest_distance"].mean()


def count_intersecting_lines(ras_xs: gpd.GeoDataFrame, nwm_reaches: gpd.GeoDataFrame) -> int:
    """Return the number of intersecting lines between the RAS cross sections and the NWM reaches."""
    if ras_xs.crs != nwm_reaches.crs:
        ras_xs = ras_xs.to_crs(nwm_reaches.crs)
    join_gdf = gpd.sjoin(ras_xs, nwm_reaches, predicate="intersects")
    return join_gdf


def filter_gdf(gdf: gpd.GeoDataFrame, column_values: list, column_name: str = "ID") -> gpd.GeoDataFrame:
    """Filter a GeoDataFrame based on the values in a column."""
    return gdf[~gdf[column_name].isin(column_values)]


# analytical functions
def walk_network(gdf: gpd.GeoDataFrame, start_id: int, stop_id: int) -> List[int]:
    """Walk the network from the start ID to the stop ID."""
    current_id = start_id
    ids = [current_id]

    while current_id != stop_id:
        result = gdf.query(f"ID == {current_id}")

        if result.empty:
            logging.error(f"No row found with ID = {current_id}")
            break

        to_value = result.iloc[0]["to_id"]
        ids.append(int(to_value))
        current_id = to_value
        if current_id == stop_id:
            break

    return ids


def calculate_conflation_metrics(
    rfc: RasFimConflater,
    candidate_reaches: gpd.GeoDataFrame,
    xs_group: gpd.GeoDataFrame,
    ras_points: gpd.GeoDataFrame,
) -> dict:
    """Calculate the conflation metrics for the candidate reaches."""
    next_round_candidates = []
    xs_hits_ids = []
    total_hits = 0
    for i in candidate_reaches.index:
        candidate_reach_points = convert_linestring_to_points(candidate_reaches.loc[i].geometry, crs=rfc.common_crs)
        # TODO: Evaluate this constant.
        if cacl_avg_nearest_points(candidate_reach_points, ras_points) < 10000:
            next_round_candidates.append(candidate_reaches.loc[i]["ID"])
            gdftmp = gpd.GeoDataFrame(geometry=[candidate_reaches.loc[i].geometry], crs=rfc.nwm_reaches.crs)
            xs_hits = count_intersecting_lines(xs_group, gdftmp)

            total_hits += xs_hits.shape[0]
            xs_hits_ids.extend(xs_hits.ID.tolist())

            logging.debug(f"conflation: {total_hits} xs hits out of {xs_group.shape[0]}")

    dangling_xs = filter_gdf(xs_group, xs_hits_ids)

    dangling_xs_interesects = gpd.sjoin(dangling_xs, rfc.nwm_reaches, predicate="intersects")

    conflation_score = round(total_hits / xs_group.shape[0], 2)

    if conflation_score == 1:
        conlfation_notes = "Probable Conflation, no dangling xs"
        manual_check_required = False

    # elif dangling_xs_interesects.shape[0] == 0:
    #     conlfation_notes = f"Probable Conflation..."
    #     manual_check_required = False

    elif conflation_score >= 0.95:
        conlfation_notes = f"Probable Conflation: partial nwm reach coverage with {dangling_xs.shape[0]}/{xs_group.shape[0]} dangling xs"
        manual_check_required = False

    elif conflation_score >= 0.25:
        conlfation_notes = f"Possible Conflation: partial nwm reach coverage with {dangling_xs.shape[0]}/{xs_group.shape[0]} dangling xs"
        manual_check_required = True

    elif conflation_score < 0.25:
        conlfation_notes = f"Unable to conflate: potential disconnected reaches with {dangling_xs.shape[0]}/{xs_group.shape[0]} dangling xs"
        manual_check_required = True

    elif conflation_score > 1:
        conlfation_notes = f"Unable to conflate: potential diverging reaches with {dangling_xs.shape[0]}/{xs_group.shape[0]} dangling xs"
        manual_check_required = True

    else:
        conlfation_notes = "Unknown error"
        manual_check_required = True

    # Convert next_round_candidates from int64 to serialize
    conlfation_metrics = {
        # "fim_reaches": [int(c) for c in next_round_candidates],
        "conflation_score": round(total_hits / xs_group.shape[0], 2),
        "conlfation_notes": conlfation_notes,
        "manual_check_required": manual_check_required,
    }
    return conlfation_metrics


def ras_xs_geometry_data(rfc: RasFimConflater, xs_id: str) -> dict:
    """Return the geometry data (max/min xs elevation) for the specified cross section."""
    # TODO: Need to verify units in the RAS data
    xs = rfc.ras_xs[rfc.ras_xs["ID"] == xs_id]
    if xs.shape[0] > 1:
        raise ValueError(f"Multiple XS found with ID = {xs_id}")

    elif xs.shape[0] == 0:
        raise ValueError(f"No XS found with ID = {xs_id}")
    else:
        xs = xs.iloc[0]

    return {"min_elevation": float(xs["thalweg"]), "max_elevation": float(xs["xs_max_elevation"])}


def get_us_most_xs_from_junction(rfc, us_river, us_reach):
    """Search for river reaches at junctions and return xs closest ds reach at junciton."""
    if rfc.ras_junctions is None:
        raise TypeError("No junctions found in the HEC-RAS model.")

    for row in rfc.ras_junctions.itertuples():
        if us_river in row.us_rivers:
            ds_rivers = row.ds_rivers.split(",")
            ds_reaches = row.ds_reaches.split(",")
            break

    if len(ds_rivers) == 0:
        raise ValueError(f"No downstream rivers found for {us_river}")
    elif len(ds_rivers) > 1:
        raise NotImplementedError(f"Multiple downstream rivers found for {us_river}")
    else:
        ds_river = ds_rivers[0]

    if len(ds_reaches) == 0:
        raise ValueError(f"No downstream reaches found for {us_river}")
    elif len(ds_reaches) > 1:
        raise NotImplementedError(f"Multiple downstream reaches found for {us_river}")
    else:
        ds_reach = ds_reaches[0]

    ds_xs_id = int(rfc.ras_xs[(rfc.ras_xs["river"] == ds_river) & (rfc.ras_xs["reach"] == ds_reach)].iloc[0].ID)
    return ds_xs_id


def map_reach_xs(rfc: RasFimConflater, reach: MultiLineString) -> dict:
    """
    Map the upstream and downstream cross sections for the nwm reach.

    TODO: This function needs helper functions to extract the datasets from the GeoDataFrames.
    """
    intersected_xs = rfc.ras_xs[rfc.ras_xs.intersects(reach.geometry)]
    has_junctions = rfc.ras_junctions is not None

    if intersected_xs.empty:
        return None
    start, end = endpoints_from_multiline(reach.geometry)

    # Begin with us_xs data
    us_xs = nearest_line_to_point(intersected_xs, start, column_id="ID")
    us_xs_station_name = str(rfc.ras_xs[rfc.ras_xs["ID"] == us_xs]["river_station"].iloc[0])
    us_river_name = rfc.ras_xs[rfc.ras_xs["ID"] == us_xs]["river"].iloc[0]
    us_reach_name = rfc.ras_xs[rfc.ras_xs["ID"] == us_xs]["reach"].iloc[0]

    # Initialize us_xs data with min /max elevation, then build the dict with added info
    us_data = ras_xs_geometry_data(rfc, us_xs)
    us_data["river"] = us_river_name
    us_data["reach"] = us_reach_name
    us_data["xs_id"] = us_xs_station_name

    # Add downstream xs data
    ds_xs = nearest_line_to_point(intersected_xs, end, column_id="ID")

    # Extend the ds_xs to the next xs
    ds_xs += 1
    if ds_xs in rfc.ras_xs["ID"]:
        # First, naievly try to get the downstream XS data assuming there is a downstream xs
        ds_xs_station_name = str(rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river_station"].iloc[0])
        ds_river_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river"].iloc[0]
        ds_reach_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["reach"].iloc[0]

        # Check  if the next xs has the same river and reach name
        if us_river_name != ds_river_name:
            try:
                ds_xs = get_us_most_xs_from_junction(rfc, us_river_name, us_reach_name)
                ds_river_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river"].iloc[0]
                ds_reach_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["reach"].iloc[0]
                ds_xs_station_name = str(rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river_station"].iloc[0])
                ds_data = ras_xs_geometry_data(rfc, ds_xs)
                logging.info(
                    f"Conflating to junction for reach {reach.ID}: {us_river_name} {us_reach_name} to {ds_river_name} {ds_reach_name}"
                )

            except ValueError as e:
                logging.warning(f"No downstream XS's: {e}")
                ds_xs_station_name = str(rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river_station"].iloc[0])
                ds_river_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river"].iloc[0]
                ds_reach_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["reach"].iloc[0]
                ds_data = ras_xs_geometry_data(rfc, ds_xs)

    # If there is no downstream XS get the last intersecting xs for the nwm reach
    else:
        logging.warning(f"No downstream XS's for {reach.ID}")
        ds_xs -= 1
        ds_xs_station_name = str(rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river_station"].iloc[0])
        ds_river_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["river"].iloc[0]
        ds_reach_name = rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]["reach"].iloc[0]

    ds_data = ras_xs_geometry_data(rfc, ds_xs)
    ds_data["xs_id"] = ds_xs_station_name
    ds_data["river"] = ds_river_name
    ds_data["reach"] = ds_reach_name

    return {
        "us_xs": us_data,
        "ds_xs": ds_data,
    }


def ras_reaches_metadata(rfc: RasFimConflater, candidate_reaches: gpd.GeoDataFrame):
    """Return the metadata for the RAS reaches."""
    reach_metadata = OrderedDict()
    for reach in candidate_reaches.itertuples():
        # logging.debug(f"REACH: {reach.ID}")
        ras_xs_data = map_reach_xs(rfc, reach)

        if ras_xs_data:
            reach_metadata[reach.ID] = ras_xs_data
        else:
            # pass dictionary with us_xs and xs_id for sorting purposes
            reach_metadata[reach.ID] = {"us_xs": {"xs_id": str(-9999)}}

    for k in reach_metadata.keys():
        flow_data = rfc.nwm_reaches[rfc.nwm_reaches["ID"] == k].iloc[0]
        if isinstance(flow_data["high_flow_threshold"], float):
            reach_metadata[k]["low_flow_cfs"] = int(round(flow_data["high_flow_threshold"], 2) * HIGH_FLOW_FACTOR)
        else:
            reach_metadata[k]["low_flow_cfs"] = -9999
            logging.warning(f"No low flow data for {k}")

        try:
            high_flow = float(flow_data["f100year"])
            reach_metadata[k]["high_flow_cfs"] = int(round(high_flow, 2))
        except:
            logging.warning(f"No high flow data for {k}")
            reach_metadata[k]["high_flow_cfs"] = -9999

        if k in rfc.local_gages.keys():
            gage_id = rfc.local_gages[k].replace(" ", "")
            reach_metadata[k]["gage"] = gage_id
            reach_metadata[k]["gage_url"] = f"https://waterdata.usgs.gov/nwis/uv?site_no={gage_id}&legacy=1"

    return reach_metadata
