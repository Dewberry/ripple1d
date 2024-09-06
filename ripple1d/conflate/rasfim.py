"""Conflation classes and functions."""

import json
import logging
import os
import sqlite3
from collections import OrderedDict
from typing import List, Tuple

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from fiona.errors import DriverError
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, Polygon, box
from shapely.ops import linemerge, nearest_points, transform

from ripple1d.consts import METERS_PER_FOOT
from ripple1d.utils.ripple_utils import xs_concave_hull

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

    Raises
    ------
        ValueError: Required layer not found in the GeoPackage
        DriverError: Unable to read the GeoPackage
    """

    def __init__(
        self, nwm_pq: str, source_model_directory: str, load_data: bool = True, output_concave_hull_path: str = None
    ):
        self.nwm_pq = nwm_pq
        self.source_model_directory = source_model_directory
        self.ras_model_name = os.path.basename(source_model_directory)
        self.ras_gpkg = os.path.join(source_model_directory,f"{self.ras_model_name}.gpkg")

        self.output_concave_hull_path = output_concave_hull_path
        self._nwm_reaches = None

        self._ras_centerlines = None
        self._ras_xs = None
        self._ras_structures = None
        self._ras_junctions = None
        self._ras_metadata=None
        self._common_crs = None
        self._xs_hulls = None
        self.__data_loaded = False
        if load_data:
            self.load_data()
            self.__data_loaded = True
            self._common_crs = NWM_CRS

    def __repr__(self):
        """Return the string representation of the object."""
        return f"RasFimConflater(nwm_pq={self.nwm_pq}, ras_gpkg={self.ras_gpkg})"


    @property
    def stac_api(self):
        """The stac_api for the HEC-RAS Model."""
        if self.ras_metadata:
            if "stac_api" in self.ras_metadata.keys():
                return self.ras_metadata["stac_api"]
    
    @property
    def stac_collection_id(self):
        """The stac_collection_id for the HEC-RAS Model."""
        if self.ras_metadata:
            if "stac_collection_id" in self.ras_metadata.keys():
                return self.ras_metadata["stac_collection_id"]
        
    
    @property
    def stac_item_id(self):
        """The stac_item_id for the HEC-RAS Model."""
        if self.ras_metadata:
            if "stac_item_id" in self.ras_metadata.keys():
                return self.ras_metadata["stac_item_id"]

    @property
    def primary_geom_file(self):
        """The primary geometry file for the HEC-RAS Model."""
        if self.ras_metadata:
            return self.ras_metadata["primary_geom_file"]

    @property
    def primary_flow_file(self):
        """The primary flow file for the HEC-RAS Model."""
        if self.ras_metadata:
            return self.ras_metadata["primary_flow_file"]
    
    @property
    def primary_plan_file(self):
        """The primary plan file for the HEC-RAS Model."""
        if self.ras_metadata:
            return self.ras_metadata["primary_plan_file"]

    @property
    def ras_project_file(self):
        """The source HEC-RAS project file."""
        if self.ras_metadata:
            return self.ras_metadata["ras_project_file"] 

    # @property
    # def xs_length_units(self):
    #     """Length units of the source HEC-RAS model."""
    #     if self.ras_metadata["units"] != "English":
    #         raise NotImplementedError(
    #             f"HEC-RAS units are {self.ras_metadata['units']}. Only 'English' units are supported at this time."
    #         )
    #     elif self.ras_metadata["units"] == "English":
    #         ras_xs=self.ras_xs
    #         ras_xs["r"]=ras_xs.apply(lambda row: self.populate_r_station(row),axis=1)
    #         if ras_xs["r"].mean()>.9 and ras_xs["r"].mean()<1.1:
    #             return "ft"
    #         elif ras_xs["r"].mean()*METERS_PER_FOOT>.9 and ras_xs["r"].mean()*METERS_PER_FOOT<1.1:
    #             raise ValueError(f"HEC-RAS units specified as English but cross section r values indicate meters")
    #         elif ras_xs["r"].mean()/5280>.9 and ras_xs["r"].mean()/5280<1.1:
    #             return "miles"
    #         else:
    #             raise ValueError(f"Unable to determine cross section length units from cross section r values")

    # @property
    # def river_station_units(self):
    #     """Station units of the source HEC-RAS model."""
    #     if self.ras_metadata["units"] != "English":
    #         raise NotImplementedError(
    #             f"HEC-RAS units are {self.ras_metadata['units']}. Only 'English' units are supported at this time."
    #         )
    #     elif self.ras_metadata["units"] == "English":
    #         ras_xs=self.ras_xs
    #         ras_xs["intersection_point"]=ras_xs.apply(lambda row: self.ras_centerlines(row.geometry).intersection(row.geometry),axis=1)
    #         ras_xs["computed_river_station"]=ras_xs.apply(lambda row: self.ras_centerlines.project(row["intersection_point"])*METERS_PER_FOOT,axis=1)
    #         ras_xs["computed_reach_length"]=ras_xs["computed_river_station"].diff()

    #         ras_xs["reach_lengths_from_original_river_station"]=ras_xs["river_station"].diff()
    #         ras_xs["reach_length_ratio"]=ras_xs["computed_reach_length"]/ras_xs["reach_lengths_from_original_river_station"]
    #         if ras_xs["reach_length_ratio"].mean()>.9 and ras_xs["reach_length_ratio"].mean()<1.1:
    #             return "ft"
    #         elif ras_xs["reach_length_ratio"].mean()*METERS_PER_FOOT>.9 and ras_xs["reach_length_ratio"].mean()*METERS_PER_FOOT<1.1:
    #             raise ValueError(f"HEC-RAS units specified as English but reach length r values indicate meters")
    #         elif ras_xs["reach_length_ratio"].mean()/5280>.9 and ras_xs["reach_length_ratio"].mean()/5280<1.1:
    #             return "miles"
    #         else:
    #             raise ValueError(f"Unable to determine reach length units from reach length r values")
            
        
    # @property
    # def flow_units(self):
    #     """Flow units of the source HEC-RAS model."""
    #     if self.gpkg_metadata["units"] != "English":
    #         raise NotImplementedError(
    #             f"HEC-RAS units are {self.ras_metadata['units']}. Only 'English' units are supported at this time."
    #         )
    #     elif self.ras_metadata["units"] == "English":
    #         return "cfs"

    def populate_r_station(self, row: pd.Series,assume_ft:bool=True) -> str:
        """Populate the r value for a cross section. The r value is the ratio of the station to actual cross section length."""
        #TODO check if this is the correct way to calculate r
        df = pd.DataFrame(row["station_elevation_points"], index=["elevation"]).T
        return df.index.max()/(row.geometry.length/METERS_PER_FOOT)

    @property
    def _gpkg_metadata(self):
        """Metadata from gpkg."""
        with sqlite3.connect(self.ras_gpkg) as conn:
            cur = conn.cursor()
            cur.execute("select * from metadata")
            return dict(cur.fetchall())

    def determine_station_order(self, xs_gdf: gpd.GeoDataFrame,reach:LineString):
        """Detemine the order based on stationing of the cross sections along the reach."""
        rs=[]
        for _, xs in xs_gdf.iterrows():
            geom=reach.intersection(xs.geometry)
            try:
                point=validate_point(geom)
                rs.append(reach.project(point))
            except TypeError as e:
                rs.append(rs[-1])
            
        xs_gdf["rs"]=rs
        return xs_gdf.sort_values(by="rs",ignore_index=True)

    def add_hull(self, xs_gdf: gpd.GeoDataFrame,reach:LineString):
        """Add the concave hull to the GeoDataFrame."""
        if len(xs_gdf) > 1:
            xs_gdf=self.determine_station_order(xs_gdf,reach)
            hull = xs_concave_hull(xs_gdf)
            if self._xs_hulls is None:
                self._xs_hulls = hull
            else:
                self._xs_hulls = pd.concat([self._xs_hulls, hull])

    def write_hulls(self):
        """Write the hulls to a GeoPackage."""
        if self._xs_hulls is not None:
            self._xs_hulls.to_file(self.output_concave_hull_path, driver="GPKG", layer="xs_concave_hulls")

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
        if "Structure" in layers:
            self._ras_structures = gpd.read_file(self.ras_gpkg, layer="Structure")
        if "metadata" in layers:
            self._ras_metadata=self._gpkg_metadata

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
    def ras_structures(self) -> gpd.GeoDataFrame:
        """RAS structures."""
        logging.info("RAS structures")
        try:
            return self._ras_structures.to_crs(self.common_crs)
        except AttributeError:
            return None
        
    @property
    @ensure_data_loaded
    def ras_junctions(self) -> gpd.GeoDataFrame:
        """RAS junctions."""
        try:
            return self._ras_junctions.to_crs(self.common_crs)
        except AttributeError:
            return None

    @property
    def ras_metadata(self) -> Polygon:
        """RAS metadata."""
        try:
            return self._ras_metadata
        except AttributeError:
            return None

    @property
    def ras_xs_bbox(self) -> Polygon:
        """Return the bounding box for the RAS cross sections."""
        return self.get_projected_bbox(self.ras_xs)

    def ras_xs_concave_hull(self, river_reach_name: str = None) -> Polygon:
        """Return the concave hull of the cross sections."""
        if river_reach_name is None:
            return xs_concave_hull(self.ras_xs)
        else:
            return xs_concave_hull(self.ras_xs[self.ras_xs["river_reach"] == river_reach_name])

    def ras_xs_convex_hull(self, river_reach_name: str = None):
        """Return the convex hull of the cross sections."""
        if river_reach_name:
            polygon = self.ras_xs["geometry"].unary_union.convex_hull

        else:
            polygon = self.ras_xs["geometry"].unary_union.convex_hull

        return gpd.GeoDataFrame({"geometry": [polygon]}, geometry="geometry", crs=self.ras_xs.crs)

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

    def local_nwm_reaches(self, river_reach_name: str = None) -> gpd.GeoDataFrame:
        """NWM reaches that intersect the RAS cross sections."""
        if river_reach_name:

            return self.nwm_reaches[
                self.nwm_reaches.intersects(self.ras_xs_convex_hull(river_reach_name)["geometry"].iloc[0])
            ]
        else:
            return self.nwm_reaches[self.nwm_reaches.intersects(self.ras_xs_convex_hull()["geometry"].iloc[0])]

    @property
    def local_gages(self) -> dict:
        """Local gages for the NWM reaches."""
        local_reaches = self.local_nwm_reaches()
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
    def ras_start_end_points(self, river_reach_name: str = None, centerline=None,clip_to_xs=False) -> Tuple[Point, Point]:
        """River_reach_name used by the decorator to get the centerline."""
        if river_reach_name:
            centerline = self.ras_centerline_by_river_reach_name(river_reach_name,clip_to_xs)
        return endpoints_from_multiline(centerline)

    def ras_centerline_by_river_reach_name(self, river_reach_name: str,clip_to_xs=False) -> LineString:
        """Return the centerline for the specified river reach."""
        if clip_to_xs:
            return self.ras_centerlines[self.ras_centerlines["river_reach"] == river_reach_name].geometry.iloc[0].intersection(self.ras_xs_concave_hull(river_reach_name).geometry.iloc[0].buffer(1))
        else:
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

    xs.rename({"thalweg": "min_elevation", "xs_max_elevation": "max_elevation", "river_station": "xs_id"}, inplace=True)
    xs_data = xs[["river", "reach", "min_elevation", "max_elevation", "xs_id"]].to_dict()

    return xs[["river", "reach", "min_elevation", "max_elevation", "xs_id"]].to_dict()


def get_us_most_xs_from_junction(rfc, us_river, us_reach):
    """Search for river reaches at junctions and return xs closest ds reach at junciton."""
    if rfc.ras_junctions is None:
        raise ValueError("No junctions found in the HEC-RAS model.")
    ds_rivers, ds_reaches = None, None
    for row in rfc.ras_junctions.itertuples():
        us_rivers = row.us_rivers.split(",")
        us_reaches = row.us_reaches.split(",")
        for river, reach in zip(us_rivers, us_reaches):

            if river == us_river and reach == us_reach:
                ds_rivers = row.ds_rivers.split(",")
                ds_reaches = row.ds_reaches.split(",")
                break

    if not ds_rivers:
        raise ValueError(f"No downstream rivers found for {us_river}")
    elif len(ds_rivers) == 0:
        raise ValueError(f"No downstream rivers found for {us_river}")
    elif len(ds_rivers) > 1:
        raise NotImplementedError(f"Multiple downstream rivers found for {us_river}")
    else:
        ds_river = ds_rivers[0]

    if not ds_reaches:
        raise ValueError(f"No downstream reaches found for {us_reach}")
    elif len(ds_reaches) == 0:
        raise ValueError(f"No downstream reaches found for {us_reach}")
    elif len(ds_reaches) > 1:
        raise NotImplementedError(f"Multiple downstream reaches found for {us_reach}")
    else:
        ds_reach = ds_reaches[0]

    ds_river_reach_gdf = rfc.ras_xs.loc[(rfc.ras_xs["river"] == ds_river) & (rfc.ras_xs["reach"] == ds_reach), :]
    max_rs = ds_river_reach_gdf.loc[:, "river_station"].max()
    ds_xs_id = int(ds_river_reach_gdf.loc[ds_river_reach_gdf["river_station"] == max_rs, "ID"].iloc[0])
    return ds_xs_id


def validate_point(geom):
    """Validate that point is of type Point. If Multipoint or Linestring create point from first coordinate pair."""
    if isinstance(geom, Point):
        return geom
    elif isinstance(geom, MultiPoint):
        return geom.geoms[0]
    elif isinstance(geom, LineString) and list(geom.coords):
        return Point(geom.coords[0])
    else:
        raise TypeError(f"expected point at xs-river intersection got: {type(geom)}")


def check_xs_direction(cross_sections: gpd.GeoDataFrame, reach: LineString):
    """Return only cross sections that are drawn right to left looking downstream."""
    ids = []
    for _, xs in cross_sections.iterrows():
        try:
            point = reach.intersection(xs["geometry"])
            point = validate_point(point)
            xs_rs = reach.project(point)

            offset = xs.geometry.offset_curve(-1)
            point = reach.intersection(offset)
            point = validate_point(point)

            offset_rs = reach.project(point)
            if xs_rs > offset_rs:
                ids.append(xs["ID"])
        except TypeError as e:
            logging.warning(f"could not validate xs-river intersection for: {xs["river"]} {xs['reach']} {xs['river_station']}")
            continue
    return cross_sections.loc[cross_sections["ID"].isin(ids)]


def map_reach_xs(rfc: RasFimConflater, reach: MultiLineString) -> dict:
    """
    Map the upstream and downstream cross sections for the nwm reach.

    TODO: This function needs helper functions to extract the datasets from the GeoDataFrames.
    """
    # get the xs that intersect the nwm reach
    intersected_xs = rfc.ras_xs[rfc.ras_xs.intersects(reach.geometry)]
    intersected_xs = check_xs_direction(intersected_xs, reach.geometry)
    has_junctions = rfc.ras_junctions is not None

    if intersected_xs.empty:
        return {"eclipsed":True}

    # get start and end points of the nwm reach
    start, end = endpoints_from_multiline(reach.geometry)

    # Begin with us_xs data
    us_xs = nearest_line_to_point(intersected_xs, start, column_id="ID")

    # Initialize us_xs data with min /max elevation, then build the dict with added info
    us_data = ras_xs_geometry_data(rfc, us_xs)

    # Add downstream xs data
    ds_xs = nearest_line_to_point(intersected_xs, end, column_id="ID")

    # get infor for the current ds_xs
    ras_river = rfc.ras_xs.loc[rfc.ras_xs["ID"] == ds_xs, "river"].iloc[0]
    ras_reach = rfc.ras_xs.loc[rfc.ras_xs["ID"] == ds_xs, "reach"].iloc[0]
    river_station = rfc.ras_xs.loc[rfc.ras_xs["ID"] == ds_xs, "river_station"].iloc[0]
    river_reach_gdf = rfc.ras_xs.loc[(rfc.ras_xs["river"] == ras_river) & (rfc.ras_xs["reach"] == ras_reach), :]

    # check if this is the most downstream xs on this ras reach
    if river_reach_gdf["river_station"].min() == river_station:
        try:
            ds_xs = get_us_most_xs_from_junction(rfc, ras_river, ras_reach)
            ds_data = ras_xs_geometry_data(rfc, ds_xs)
            logging.debug(
                f"Conflating to junction for reach {reach.ID}: {us_data['river']} {us_data['reach']} to {ds_data['river']} {ds_data['reach']}"
            )
        except ValueError as e:
            ds_data = ras_xs_geometry_data(rfc, ds_xs)
            logging.warning(f"No downstream XS's: {e}")

    # if it is not the most downstream xs on this ras reach then compute the next downstream river station and determine the id
    else:
        ds_xs_gdf = river_reach_gdf.loc[river_reach_gdf["river_station"] < river_station].reset_index(drop=True)
        if len(ds_xs_gdf) == 1:
            index = 0
        else:
            index = ds_xs_gdf["river_station"].idxmax()
        ds_xs = ds_xs_gdf["ID"].iloc[index]
        ds_data = ras_xs_geometry_data(rfc, ds_xs)

    # add xs concave hull
    if rfc.output_concave_hull_path:
        xs_gdf = pd.concat([intersected_xs, rfc.ras_xs[rfc.ras_xs["ID"] == ds_xs]], ignore_index=True)
        rfc.add_hull(xs_gdf,reach.geometry)
    
    if us_data==ds_data:
        return {"eclipsed":True}

    return {
        "us_xs": us_data,
        "ds_xs": ds_data,
        "eclipsed":False
    }


def ras_reaches_metadata(rfc: RasFimConflater, candidate_reaches: gpd.GeoDataFrame):
    """Return the metadata for the RAS reaches."""
    reach_metadata = OrderedDict()
    hulls = []
    for reach in candidate_reaches.itertuples():
        # logging.debug(f"REACH: {reach.ID}")

        # get the xs data for the reach
        ras_xs_data = map_reach_xs(rfc, reach)

        reach_metadata[reach.ID] = ras_xs_data

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
        try:
            reach_metadata[k]["network_to_id"] = str(flow_data["to_id"])
        except:
            logging.warning(f"No to_id data for {k}")
            reach_metadata[k]["network_to_id"] = "-9999"

        if k in rfc.local_gages.keys():
            gage_id = rfc.local_gages[k].replace(" ", "")
            reach_metadata[k]["gage"] = gage_id
            reach_metadata[k]["gage_url"] = f"https://waterdata.usgs.gov/nwis/uv?site_no={gage_id}&legacy=1"

    return reach_metadata
