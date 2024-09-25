import json
import logging
import os
import sqlite3
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from shapely import Geometry, LineString, MultiLineString, MultiPoint, Point
from shapely.ops import linemerge

from ripple1d.consts import HYDROFABRIC_CRS, METERS_PER_FOOT
from ripple1d.data_model import XS
from ripple1d.ops.subset_gpkg import RippleGeopackageSubsetter
from ripple1d.utils.ripple_utils import fix_reversed_xs, xs_concave_hull


class ConflationMetrics:
    """Calculate metrics for a cross section."""

    def __init__(
        self,
        xs_gdf: gpd.GeoDataFrame,
        river_gdf: gpd.GeoDataFrame,
        hull_gdf,
        network_reach: LineString,
        network_reach_plus_ds_reach: LineString,
    ):
        self.xs_gdf = xs_gdf
        self.river_gdf = river_gdf
        self.network_reach = network_reach
        self.network_reach_plus_ds_reach = network_reach_plus_ds_reach
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
        """Calculate the distance between the thalweg point and the network intersection point."""
        try:
            xs_gdf["station_elevation"] = xs_gdf.apply(lambda row: self.populate_station_elevation(row), axis=1)
            xs_gdf["thalweg_station"] = xs_gdf.apply(lambda row: self.populate_thalweg_station(row), axis=1)
            xs_gdf["thalweg_point"] = xs_gdf.apply(
                lambda row: row.geometry.interpolate(row["thalweg_station"] * METERS_PER_FOOT), axis=1
            )

            xs_gdf["ras_intersection_point"] = None
            for _, r in self.river_gdf.iterrows():
                xs_gdf.loc[xs_gdf["river_reach"] == r["river_reach"], "ras_intersection_point"] = xs_gdf.loc[
                    xs_gdf["river_reach"] == r["river_reach"], :
                ].apply(lambda row: r.geometry.intersection(row.geometry), axis=1)

            xs_gdf["network_intersection_point"] = xs_gdf.apply(
                lambda row: self.network_reach_plus_ds_reach.intersection(row.geometry), axis=1
            )

            xs_gdf["centerline_offset"] = xs_gdf.apply(
                lambda row: row["ras_intersection_point"].distance(row["network_intersection_point"]), axis=1
            )
            xs_gdf["thalweg_offset"] = xs_gdf.apply(
                lambda row: row["thalweg_point"].distance(row["network_intersection_point"]), axis=1
            )

            return {
                "centerline_offset": xs_gdf["centerline_offset"].describe().round().astype(int).to_dict(),
                "thalweg_offset": xs_gdf["thalweg_offset"].describe().round().astype(int).to_dict(),
            }
        except Exception as e:
            logging.error(f"Error: {e}")
            logging.error(f"traceback: {traceback.format_exc()}")

    def ensure_point(self, geom: Geometry):
        """Ensure that the geometry is Point."""
        if isinstance(geom, MultiPoint):
            return list(geom.geoms)[0]
        elif isinstance(geom, Point):
            return geom
        elif isinstance(geom, LineString):
            return list(geom.boundary.geoms)[0]
        else:
            raise ValueError(f"Not a valid geometry type: {type(geom)}. Only Point, MultiPoint,LineString supported.")

    def length_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the reach length between cross sections along the ras river line and the network reach."""
        try:
            xs_gdf = xs_gdf[
                (xs_gdf["river_station"] == xs_gdf["river_station"].max())
                | (xs_gdf["river_station"] == xs_gdf["river_station"].min())
            ]
            xs_gdf["network_intersection_point"] = xs_gdf.apply(
                lambda row: self.network_reach_plus_ds_reach.intersection(row.geometry), axis=1
            )
            xs_gdf["network_intersection_point"] = xs_gdf.apply(
                lambda row: self.ensure_point(row["network_intersection_point"]), axis=1
            )

            xs_gdf["network_station"] = xs_gdf.apply(
                lambda row: self.network_reach_plus_ds_reach.project(row["network_intersection_point"]), axis=1
            )
            network_length = xs_gdf["network_station"].max() - xs_gdf["network_station"].min()

            if len(xs_gdf["river_reach"].unique()) > 1:
                raise ValueError("Cross sections must all be on the same river reach.")
            else:
                river_line = self.river_gdf.loc[
                    self.river_gdf["river_reach"] == xs_gdf["river_reach"].iloc[0], "geometry"
                ].iloc[0]
                xs_gdf["ras_intersection_point"] = xs_gdf.apply(
                    lambda row: river_line.intersection(row.geometry), axis=1
                )
                xs_gdf["ras_station"] = xs_gdf.apply(
                    lambda row: river_line.project(row["ras_intersection_point"]), axis=1
                )
                ras_length = xs_gdf["ras_station"].max() - xs_gdf["ras_station"].min()

                network_ras_ratio = network_length / ras_length
            return {
                "ras": int(ras_length / METERS_PER_FOOT),
                "network": int(network_length / METERS_PER_FOOT),
                "network_to_ras_ratio": round(float(network_ras_ratio), 2),
            }
        except Exception as e:
            logging.error(f"Error: {e}")
            logging.error(f"traceback: {traceback.format_exc()}")

    # def parrallel_reaches(self, network_reaches: gpd.GeoDataFrame) -> dict:
    #     """Calculate the overlap between the network reach and the cross sections."""
    #     if network_reaches.empty:
    #         return []
    #     overlaps = network_reaches[network_reaches.intersects(self.xs_gdf.union_all())]
    #     if overlaps.empty:
    #         return []
    #     overlaps["overlap"] = overlaps.apply(
    #         lambda row: int(
    #             row["geometry"].intersection(xs_concave_hull(self.xs_gdf)["geometry"].iloc[0]).length / METERS_PER_FOOT
    #         ),
    #         axis=1,
    #     )

    #     return [{"id": str(row["ID"]), "overlap": row["overlap"]} for _, row in overlaps.iterrows()]

    def overlapped_reaches(self, to_reaches: gpd.GeoDataFrame) -> dict:
        """Calculate the overlap between the network reach and the cross sections."""
        if to_reaches.empty:
            return []
        geom_name = to_reaches.geometry.name
        for i, row in to_reaches.iterrows():
            if row[geom_name].intersects(self.xs_gdf.union_all()):
                overlap = (
                    row[geom_name]
                    .intersection(xs_concave_hull(fix_reversed_xs(self.xs_gdf, self.river_gdf))["geometry"].iloc[0])
                    .length
                    / METERS_PER_FOOT
                )
                return [{"id": str(row["ID"]), "overlap": int(overlap)}]
        return []

    def eclipsed_reaches(self, network_reaches: gpd.GeoDataFrame) -> dict:
        """Calculate the overlap between the network reach and the cross sections."""
        if network_reaches.empty:
            return []
        eclipsed_reaches = network_reaches[
            network_reaches.covered_by(
                xs_concave_hull(fix_reversed_xs(self.xs_gdf, self.river_gdf))["geometry"].iloc[0]
            )
        ]

        return [str(row["ID"]) for _, row in eclipsed_reaches.iterrows()]

    def compute_coverage_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the coverage metrics for a set of cross sections."""
        try:
            xs_gdf = xs_gdf[
                (xs_gdf["river_station"] == xs_gdf["river_station"].max())
                | (xs_gdf["river_station"] == xs_gdf["river_station"].min())
            ]
            xs_gdf["intersection_point"] = xs_gdf.apply(
                lambda row: self.network_reach_plus_ds_reach.intersection(row.geometry), axis=1
            )
            xs_gdf["intersection_point"] = xs_gdf.apply(
                lambda row: self.ensure_point(row["intersection_point"]), axis=1
            )

            xs_gdf["station_percent"] = xs_gdf.apply(
                lambda row: self.network_reach_plus_ds_reach.project(row["intersection_point"])
                / self.network_reach.length,
                axis=1,
            )
            return {
                "start": float(round(xs_gdf["station_percent"].min(), 2)),
                "end": min([round(xs_gdf["station_percent"].max(), 2), 1]),
            }
        except Exception as e:
            logging.error(f"Error: {e}")
            logging.error(f"traceback: {traceback.format_exc()}")


def compute_conflation_metrics(source_model_directory: str, source_network: str, task_id: str = ""):
    """Compute metrics for a network reach."""
    logging.info(f"{task_id} | compute_conflation_metrics starting")
    network_pq_path = source_network["file_name"]
    model_name = os.path.basename(source_model_directory)
    src_gpkg_path = os.path.join(source_model_directory, f"{model_name}.gpkg")
    conflation_json = os.path.join(source_model_directory, f"{model_name}.conflation.json")
    conflation_parameters = json.load(open(conflation_json))
    rgs = RippleGeopackageSubsetter(src_gpkg_path, conflation_json, "")

    for network_id in conflation_parameters["reaches"].keys():
        try:
            if conflation_parameters["reaches"][network_id]["eclipsed"] == True:
                continue

            rgs.set_nwm_id(network_id)
            layers = {}
            for layer, gdf in rgs.subset_gdfs.items():
                layers[layer] = gdf.to_crs(HYDROFABRIC_CRS)

            network_reaches = gpd.read_parquet(network_pq_path, bbox=layers["XS"].total_bounds)
            network_reach = linemerge(network_reaches.loc[network_reaches["ID"] == int(network_id)].geometry.iloc[0])
            network_reach_plus_ds_reach = combine_reaches(network_reaches, network_id)

            cm = ConflationMetrics(
                fix_reversed_xs(layers["XS"], layers["River"]),
                layers["River"],
                rgs.ripple_xs_concave_hull,
                network_reach,
                network_reach_plus_ds_reach,

            metrics = {
                "xs": cm.thalweg_metrics(layers["XS"]),
                "lengths": cm.length_metrics(layers["XS"]),
                "coverage": cm.compute_coverage_metrics(layers["XS"]),
            }

            to_id = conflation_parameters["reaches"][network_id]["network_to_id"]
            if to_id in conflation_parameters["reaches"].keys():
                next_to_id = int(conflation_parameters["reaches"][to_id]["network_to_id"])
            else:
                next_to_id = None

            overlapped_reaches = cm.overlapped_reaches(
                network_reaches[network_reaches["ID"].isin([int(to_id), next_to_id])]
            )
            eclipsed_reaches = cm.eclipsed_reaches(network_reaches[network_reaches["ID"] != int(network_id)])

            conflation_parameters["reaches"][network_id].update({"metrics": metrics})
            conflation_parameters["reaches"][network_id].update({"overlapped_reaches": overlapped_reaches})
            conflation_parameters["reaches"][network_id].update({"eclipsed_reaches": eclipsed_reaches})
            conflation_parameters["metadata"]["length_units"] = "feet"
            conflation_parameters["metadata"]["flow_units"] = "cfs"
        except Exception as e:
            logging.error(f"Error: {e}")
            logging.error(f"traceback: {traceback.format_exc()}")
            conflation_parameters["reaches"][network_id].update({"metrics": {}})
    with open(conflation_json, "w") as f:
        f.write(json.dumps(conflation_parameters, indent=4))

    logging.info(f"{task_id} | compute_conflation_metrics complete")
    return conflation_parameters


def combine_reaches(network_reaches: gpd.GeoDataFrame, network_id: str) -> LineString:
    """Combine network reaches."""
    reach = network_reaches.loc[network_reaches["ID"] == int(network_id), :]
    to_reach = network_reaches.loc[network_reaches["ID"] == int(reach["to_id"].iloc[0]), :]

    if to_reach.empty:
        return linemerge(reach.geometry.iloc[0])
    else:
        next_to_reach = network_reaches.loc[network_reaches["ID"] == int(to_reach["to_id"].iloc[0]), :]
        if next_to_reach.empty:
            return linemerge(MultiLineString([linemerge(reach.geometry.iloc[0]), linemerge(to_reach.geometry.iloc[0])]))
        else:
            return linemerge(
                MultiLineString(
                    [
                        linemerge(reach.geometry.iloc[0]),
                        linemerge(to_reach.geometry.iloc[0]),
                        linemerge(next_to_reach.geometry.iloc[0]),
                    ]
                )
            )
