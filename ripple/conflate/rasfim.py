import json
from collections import OrderedDict
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from fiona.errors import DriverError
from shapely.geometry import LineString, MultiLineString, Point, Polygon, box
from shapely.ops import linemerge, nearest_points, transform

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


class RasFimConflater:
    """
    Conflate NWM and RAS data for a single river reach

    Args:
        nwm_parquet (str): Path to the NWM Parquet file converted to parquet from:
            s3://noaa-nws-owp-fim/ras2fim/inputs/X-National_Datasets/nwm_flows.gpkg
        ras_gpkg (str): Path to the RAS GeoPackage
        load_data (bool, optional): Load the data on initialization. Defaults to True.
        bucket (str, optional): S3 bucket to read data from. Defaults to "fim".

    Raises:
        ValueError: Required layer not found in the GeoPackage
        DriverError: Unable to read the GeoPackage
    """

    def __init__(
        self, nwm_pq: str, ras_gpkg: str, load_data: bool = True, bucket="fim"
    ):
        self.nwm_pq = nwm_pq
        self.ras_gpkg = ras_gpkg
        self.bucket = bucket
        self._nwm_reaches = None

        self._ras_centerlines = None
        self._ras_xs = None
        self._common_crs = None
        self.__data_loaded = False
        if load_data:
            self.load_data()
            self.__data_loaded = True
            self._common_crs = NWM_CRS

    def __repr__(self):
        return f"RasFimConflater(nwm_pq={self.nwm_pq}, ras_gpkg={self.ras_gpkg})"

    def load_gpkg_layer(self, gpkg: str, layer: str):
        try:
            return gpd.read_file(gpkg, layer=layer)
        except Exception as e:
            if type(e) == ValueError:
                raise ValueError(f"Required layer '{layer}' not found in {gpkg}")
            elif type(e) == DriverError:
                raise DriverError(f"Unable to read {gpkg}")
            else:
                raise (e)

    def load_pq(self, nwm_pq: str):
        try:
            return gpd.read_parquet(nwm_pq)
        except Exception as e:
            if type(e) == DriverError:
                raise DriverError(f"Unable to read {nwm_pq}")
            else:
                raise (e)

    def load_data(self):
        """
        Loads the NWM and RAS data from the GeoPackages
        """
        self._nwm_reaches = self.load_pq(self.nwm_pq)
        self._ras_centerlines = self.load_gpkg_layer(self.ras_gpkg, "Rivers")
        self._ras_xs = self.load_gpkg_layer(self.ras_gpkg, "XS")

    def ensure_data_loaded(func):
        def wrapper(self, *args, **kwargs):
            if not self.__data_loaded:
                raise Exception("Data not loaded")
            return func(self, *args, **kwargs)

        return wrapper

    @property
    def common_crs(self):
        return self._common_crs

    @property
    @ensure_data_loaded
    def ras_centerlines(self) -> gpd.GeoDataFrame:
        return self._ras_centerlines.to_crs(self.common_crs)

    @property
    @ensure_data_loaded
    def ras_river_reach_names(self) -> List[str]:
        return self.ras_centerlines.id.unique().tolist()

    @property
    @ensure_data_loaded
    def ras_xs(self) -> gpd.GeoDataFrame:
        return self._ras_xs.to_crs(self.common_crs)

    @property
    def ras_xs_bbox(self) -> Polygon:
        return self.get_projected_bbox(self.ras_xs)

    @property
    def ras_banks(self):
        raise NotImplementedError

    def get_projected_bbox(self, gdf: gpd.GeoDataFrame) -> Polygon:
        assert gdf.crs, "GeoDataFrame must have a CRS"
        project = pyproj.Transformer.from_crs(
            gdf.crs, NWM_CRS, always_xy=True
        ).transform
        return transform(project, box(*tuple(gdf.total_bounds)))

    @property
    @ensure_data_loaded
    def nwm_reaches(self) -> gpd.GeoDataFrame:
        return self._nwm_reaches

    @property
    def local_nwm_reaches(self) -> gpd.GeoDataFrame:
        return self.nwm_reaches[self.nwm_reaches.intersects(self.ras_xs_bbox)]

    @property
    def local_gages(self) -> dict:
        local_reaches = self.local_nwm_reaches
        gages = local_reaches["gages"]
        reach_ids = local_reaches["ID"]
        return {
            reach_id: gage
            for reach_id, gage in zip(reach_ids, gages)
            if pd.notna(gage) and gage.strip()
        }

    def local_lakes(self):
        raise NotImplementedError

    def check_centerline(func):
        """
        Helper function to ensure that ras_centerline is available and that the centerline is specified
        If the centerline is not specified, the first centerline is used, with a check that there is only one centerline
        """

        def wrapper(self, *args, **kwargs):
            assert self.__data_loaded, "Data not loaded"

            river_reach_name = kwargs.get("river_reach_name", None)
            if river_reach_name:
                print(
                    "No river_reach_name specified, using first and only centerline found."
                )
                centerline = self.ras_centerline_by_river_reach_name(river_reach_name)
            else:
                if self.ras_centerlines.shape[0] == 1:
                    centerline = self.ras_centerlines.geometry.iloc[0]
                else:
                    raise ValueError(
                        "Multiple centerlines found, please specify river_reach_name"
                    )
            kwargs["centerline"] = centerline
            return func(self, *args, **kwargs)

        return wrapper

    @check_centerline
    def ras_start_end_points(
        self, river_reach_name: str = None, centerline=None
    ) -> Tuple[Point, Point]:
        """
        river_reach_name used by the decorator to get the centerline
        """
        return endpoints_from_multiline(centerline)


# general geospatial functions
def endpoints_from_multiline(mline: MultiLineString) -> Tuple[Point, Point]:
    merged_line = linemerge(mline)
    start_point, end_point = merged_line.coords[0], merged_line.coords[-1]
    return Point(start_point), Point(end_point)


def nearest_line_to_point(
    lines: gpd.GeoDataFrame, point: Point, column_id: str = "ID"
) -> int:
    if not column_id in lines.columns:
        raise ValueError("required `ID` column not found in GeoDataFrame")

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
    num_points = int(round(linestring.length / point_spacing))

    if num_points == 0:
        num_points = 1

    points = [
        linestring.interpolate(distance)
        for distance in np.linspace(0, linestring.length, num_points)
    ]

    return gpd.GeoDataFrame(
        geometry=points,
        crs=crs,
    )


def cacl_avg_nearest_points(
    reference_gdf: gpd.GeoDataFrame, compare_points_gdf: gpd.GeoDataFrame
) -> float:
    multipoint = compare_points_gdf.geometry.unary_union
    reference_gdf["nearest_distance"] = reference_gdf.geometry.apply(
        lambda point: point.distance(nearest_points(point, multipoint)[1])
    )
    return reference_gdf["nearest_distance"].mean()


def count_intersecting_lines(
    ras_xs: gpd.GeoDataFrame, nwm_reaches: gpd.GeoDataFrame
) -> int:
    if ras_xs.crs != nwm_reaches.crs:
        ras_xs = ras_xs.to_crs(nwm_reaches.crs)
    join_gdf = gpd.sjoin(ras_xs, nwm_reaches, predicate="intersects")
    return join_gdf


def filter_gdf(
    gdf: gpd.GeoDataFrame, column_values: list, column_name: str = "id"
) -> gpd.GeoDataFrame:
    return gdf[~gdf[column_name].isin(column_values)]


# analytical functions
def walk_network(gdf: gpd.GeoDataFrame, start_id: int, stop_id: int) -> List[int]:
    current_id = start_id
    ids = [current_id]

    while current_id != stop_id:
        result = gdf.query(f"ID == {current_id}")

        if result.empty:
            print(f"No row found with ID = {current_id}")
            break

        to_value = result.iloc[0]["to"]
        ids.append(to_value)
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
    next_round_candidates = []
    xs_hits_ids = []
    total_hits = 0
    for i in candidate_reaches.index:
        candidate_reach_points = convert_linestring_to_points(
            candidate_reaches.loc[i].geometry, crs=rfc.common_crs
        )
        if cacl_avg_nearest_points(candidate_reach_points, ras_points) < 2000:
            next_round_candidates.append(candidate_reaches.loc[i]["ID"])
            gdftmp = gpd.GeoDataFrame(
                geometry=[candidate_reaches.loc[i].geometry], crs=rfc.nwm_reaches.crs
            )
            xs_hits = count_intersecting_lines(xs_group, gdftmp)

            total_hits += xs_hits.shape[0]
            xs_hits_ids.extend(xs_hits.id.tolist())

            # print(total_hits, xs_group.shape[0])

    dangling_xs = filter_gdf(xs_group, xs_hits_ids)

    dangling_xs_interesects = gpd.sjoin(
        dangling_xs, rfc.nwm_reaches, predicate="intersects"
    )

    conflation_score = round(total_hits / xs_group.shape[0], 2)

    if conflation_score == 1:
        conlfation_notes = "Probable Conflation, no dangling xs"
        manual_check_required = False

    elif dangling_xs_interesects.shape[0] == 0:
        conlfation_notes = f"Probable Conflation. Score = {conflation_score}% with {dangling_xs.shape[0]} dangling xs"
        manual_check_required = False

    elif conflation_score >= 0.25:
        conlfation_notes = "Possible Conflation: partial nwm reach coverage"
        manual_check_required = True

    elif conflation_score < 0.25:
        conlfation_notes = "Unable to conflate: potential disconnected reaches"
        manual_check_required = True

    elif conflation_score > 1:
        conlfation_notes = "Unable to conflate: potential diverging reaches"
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
    # TODO: Need to verify units in the RAS data
    xs = rfc.ras_xs[rfc.ras_xs["id"] == xs_id]
    multiline = xs.geometry.values[0]
    if isinstance(multiline, MultiLineString):
        for linestring in multiline.geoms:
            min_el = min([p[2] for p in linestring.coords])
            max_el = max([p[2] for p in linestring.coords])
    return {"min_el": min_el, "max_el": max_el}


def map_reach_xs(rfc: RasFimConflater, reach: MultiLineString):
    intersected_xs = rfc.ras_xs[rfc.ras_xs.intersects(reach)]
    if intersected_xs.empty:
        return None
    start, end = endpoints_from_multiline(reach)

    up_xs = nearest_line_to_point(intersected_xs, start, column_id="id")
    ds_xs = nearest_line_to_point(intersected_xs, end, column_id="id")

    us_data = ras_xs_geometry_data(rfc, up_xs)
    ds_data = ras_xs_geometry_data(rfc, ds_xs)

    us_data["xs_id"] = up_xs
    ds_data["xs_id"] = ds_xs
    return {
        "up_xs": us_data,
        "ds_xs": ds_data,
    }


def ras_reaches_metadata(
    rfc: RasFimConflater, low_flow_df: pd.DataFrame, candidate_reaches: gpd.GeoDataFrame
):
    reach_metadata = OrderedDict()
    for reach in candidate_reaches.itertuples():
        reach_geom = reach.geometry
        ras_xs_data = map_reach_xs(rfc, reach_geom)
        if ras_xs_data:
            reach_metadata[reach.ID] = ras_xs_data
        else:
            # pass dictionary with up_xs and xs_id for sorting purposes
            reach_metadata[reach.ID] = {"up_xs": {"xs_id": -9999}}

    for k in reach_metadata.keys():
        low_flow = low_flow_df[low_flow_df.feature_id == k]
        try:
            reach_metadata[k]["low_flow_cfs"] = round(
                low_flow.iloc[0]["discharge_cfs"], 2
            )
        except IndexError as e:
            print(f"warning 1: no low flow data for reach {k}: error {e}")
            reach_metadata[k]["low_flow_cfs"] = -9999

        except TypeError as e:
            print(f"warning 1: no low flow data for reach {k}: error {e}")

        if k in rfc.local_gages.keys():
            gage_id = rfc.local_gages[k].replace(" ", "")
            reach_metadata[k]["gage"] = gage_id
            reach_metadata[k][
                "gage_url"
            ] = f"https://waterdata.usgs.gov/nwis/uv?site_no={gage_id}&legacy=1"

    sorted_data = dict(
        sorted(
            reach_metadata.items(),
            key=lambda item: int(item[1]["up_xs"]["xs_id"]),
            reverse=True,
        )
    )

    return sorted_data
