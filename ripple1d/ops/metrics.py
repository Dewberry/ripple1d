import json

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from shapely import LineString, Point
from shapely.ops import linemerge

from ripple1d.data_model import XS
from ripple1d.ops.subset_gpkg import RippleGeopackageSubsetter


class ConflationMetrics:
    """Calculate metrics for a cross section."""

    def __init__(self, xs_gdf: gpd.GeoDataFrame, river_gdf: gpd.GeoDataFrame, nwm_reach: LineString):
        self.xs_gdf = xs_gdf
        self.river_gdf = river_gdf
        self.nwm_reach = nwm_reach
        self.crs = xs_gdf.crs

    def populate_station_elevation(self, row: pd.Series) -> dict:
        """Populate the station elevation for a cross section."""
        xs = XS(row.ras_data.splitlines(), row.river_reach, row.river, row.reach, self.crs)
        return dict(xs.station_elevation_points)

    def populate_thalweg_station(self, row: pd.Series) -> str:
        """Populate the thalweg station for a cross section."""
        df = pd.DataFrame(row["station_elevation"], index=["elevation"]).T
        return df.loc[df["elevation"] == row.thalweg].index[0]

    def thalweg_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the distance between the thalweg point and the NWM intersection point."""
        xs_gdf["station_elevation"] = xs_gdf.apply(lambda row: self.populate_station_elevation(row), axis=1)
        xs_gdf["thalweg_station"] = xs_gdf.apply(lambda row: self.populate_thalweg_station(row), axis=1)
        xs_gdf["thalweg_point"] = xs_gdf.apply(lambda row: row.geometry.interpolate(row["thalweg_station"]), axis=1)

        xs_gdf["ras_intersection_point"] = None
        for _, r in self.river_gdf.iterrows():
            xs_gdf.loc[xs_gdf["river_reach"] == r["river_reach"], "ras_intersection_point"] = xs_gdf.loc[
                xs_gdf["river_reach"] == r["river_reach"], :
            ].apply(lambda row: r.geometry.intersection(row.geometry), axis=1)

        xs_gdf["nwm_intersection_point"] = xs_gdf.apply(lambda row: self.nwm_reach.intersection(row.geometry), axis=1)

        xs_gdf["intersection_delta_xy"] = xs_gdf.apply(
            lambda row: row["ras_intersection_point"].distance(row["nwm_intersection_point"]), axis=1
        )
        xs_gdf["thalweg_delta_xy"] = xs_gdf.apply(
            lambda row: row["thalweg_point"].distance(row["nwm_intersection_point"]), axis=1
        )

        return {
            "intersection_delta_xy": xs_gdf["intersection_delta_xy"]
            .describe(np.linspace(0.1, 1, 10))
            .round()
            .to_dict(),
            "thalweg_delta_xy": xs_gdf["thalweg_delta_xy"].describe(np.linspace(0.1, 1, 10)).round().to_dict(),
        }

    def reach_length_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the reach length between cross sections along the ras river line and the NWM reach."""
        xs_gdf["nwm_intersection_point"] = xs_gdf.apply(lambda row: self.nwm_reach.intersection(row.geometry), axis=1)
        xs_gdf["nwm_station"] = xs_gdf.apply(lambda row: self.nwm_reach.project(row["nwm_intersection_point"]), axis=1)
        xs_gdf["nwm_reach_length"] = xs_gdf["nwm_station"].diff()

        if len(xs_gdf["river_reach"].unique()) > 1:
            raise ValueError("Cross sections must all be on the same river reach.")
        else:
            river_line = self.river_gdf.loc[
                self.river_gdf["river_reach"] == xs_gdf["river_reach"].iloc[0], "geometry"
            ].iloc[0]
            xs_gdf["ras_intersection_point"] = xs_gdf.apply(lambda row: river_line.intersection(row.geometry), axis=1)
            xs_gdf["ras_station"] = xs_gdf.apply(lambda row: river_line.project(row["ras_intersection_point"]), axis=1)
            xs_gdf["ras_reach_length"] = xs_gdf["ras_station"].diff()

            xs_gdf["nwm_to_ras_ratio"] = xs_gdf["nwm_reach_length"] / xs_gdf["ras_reach_length"]
        return {
            "ras": xs_gdf["ras_reach_length"].describe(np.linspace(0.1, 1, 10)).round().to_dict(),
            "nwm": xs_gdf["nwm_reach_length"].describe(np.linspace(0.1, 1, 10)).round().to_dict(),
            "nwm_to_ras_ratio": xs_gdf["nwm_to_ras_ratio"].describe(np.linspace(0.1, 1, 10)).round(2).to_dict(),
        }


def compute_conflation_metrics(src_gpkg_path: str, nwm_pq_path: str, conflation_json: str):
    """Compute metrics for a nwm reach."""
    conflation_parameters = json.load(open(conflation_json))

    for nwm_id in conflation_parameters["reaches"].keys():

        rgs = RippleGeopackageSubsetter(src_gpkg_path, conflation_json, "", nwm_id)
        layers = {}
        for layer, gdf in rgs.subset_gdfs.items():
            layers[layer] = gdf.to_crs(5070)

        nwm_reaches = gpd.read_parquet(nwm_pq_path, bbox=layers["XS"].total_bounds)
        nwm_reach = combine_reaches(nwm_reaches, nwm_id)

        cm = ConflationMetrics(layers["XS"], layers["River"], nwm_reach)

        metrics = {"thalweg": cm.thalweg_metrics(layers["XS"]), "reach_length": cm.reach_length_metrics(layers["XS"])}

        conflation_parameters["reaches"][nwm_id].update({"metrics": metrics})

    with open(conflation_json, "w") as f:
        f.write(json.dumps(conflation_parameters, indent=4))
    return conflation_parameters


def combine_reaches(nwm_reaches: gpd.GeoDataFrame, nwm_id: str) -> LineString:
    """Combine NWM reaches."""
    reach = nwm_reaches.loc[nwm_reaches["ID"] == int(nwm_id), :]
    to_reach = nwm_reaches.loc[nwm_reaches["ID"] == int(reach["to_id"].iloc[0]), :]
    if to_reach.empty:
        return LineString(linemerge(reach.geometry.iloc[0]).coords)
    else:
        return LineString(
            list(linemerge(reach.geometry.iloc[0]).coords) + list(linemerge(to_reach.geometry.iloc[0]).coords)
        )
