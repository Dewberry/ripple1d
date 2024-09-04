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

        network_reach: LineString,
        self.xs_gdf = xs_gdf
        self.river_gdf = river_gdf
        self.network_reach = network_reach
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
        xs_gdf["station_elevation"] = xs_gdf.apply(lambda row: self.populate_station_elevation(row), axis=1)
        xs_gdf["thalweg_station"] = xs_gdf.apply(lambda row: self.populate_thalweg_station(row), axis=1)
        xs_gdf["thalweg_point"] = xs_gdf.apply(lambda row: row.geometry.interpolate(row["thalweg_station"]), axis=1)

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
            "centerline_offset": xs_gdf["centerline_offset"]
            .describe(np.linspace(0.1, 1, 10))
            .round()
            .astype(int)
            .to_dict(),
            "thalweg_offset": xs_gdf["thalweg_offset"].describe(np.linspace(0.1, 1, 10)).round().astype(int).to_dict(),
        }

    def length_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the reach length between cross sections along the ras river line and the network reach."""
        xs_gdf["network_intersection_point"] = xs_gdf.apply(
            lambda row: self.network_reach_plus_ds_reach.intersection(row.geometry), axis=1
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
            xs_gdf["ras_intersection_point"] = xs_gdf.apply(lambda row: river_line.intersection(row.geometry), axis=1)
            xs_gdf["ras_station"] = xs_gdf.apply(lambda row: river_line.project(row["ras_intersection_point"]), axis=1)
            ras_length = xs_gdf["ras_station"].max() - xs_gdf["ras_station"].min()

            network_ras_ratio = network_length / ras_length
        return {
            "ras": int(ras_length / METERS_PER_FOOT),
            "network": int(network_length / METERS_PER_FOOT),
            "network_to_ras_ratio": round(network_ras_ratio, 2),
        }

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

    def overlapped_reaches(self, to_reach: gpd.GeoDataFrame) -> dict:
        """Calculate the overlap between the network reach and the cross sections."""
        if to_reach.empty:
            return []
        if to_reach["geometry"].iloc[0].intersects(self.xs_gdf.union_all()):
            overlap = (
                to_reach["geometry"].intersection(xs_concave_hull(self.xs_gdf)["geometry"].iloc[0]).length
                / METERS_PER_FOOT
            )
        return [{"id": str(to_reach["ID"]), "overlap": overlap}]

    def eclipsed_reaches(self, network_reaches: gpd.GeoDataFrame) -> dict:
        """Calculate the overlap between the network reach and the cross sections."""
        if network_reaches.empty:
            return []
        eclipsed_reaches = network_reaches[network_reaches.covered_by(xs_concave_hull(self.xs_gdf)["geometry"].iloc[0])]

        return [str(row["ID"]) for _, row in eclipsed_reaches.iterrows()]

    def compute_coverage_metrics(self, xs_gdf: gpd.GeoDataFrame) -> dict:
        """Calculate the coverage metrics for a set of cross sections."""
        xs_gdf["intersection_point"] = xs_gdf.apply(
            lambda row: self.network_reach_plus_ds_reach.intersection(row.geometry), axis=1
        )
        xs_gdf["station_percent"] = xs_gdf.apply(
            lambda row: self.network_reach_plus_ds_reach.project(row["intersection_point"]) / self.network_reach.length,
            axis=1,
        )
        return {"start": xs_gdf["station_percent"].min().round(2), "end": xs_gdf["station_percent"].max().round(2)}


def compute_conflation_metrics(src_gpkg_path: str, network_pq_path: str, conflation_json: str):
    """Compute metrics for a network reach."""
    conflation_parameters = json.load(open(conflation_json))

    for network_id in conflation_parameters["reaches"].keys():

        rgs = RippleGeopackageSubsetter(src_gpkg_path, conflation_json, "", network_id)
        layers = {}
        for layer, gdf in rgs.subset_gdfs.items():
            layers[layer] = gdf.to_crs(HYDROFABRIC_CRS)

        network_reaches = gpd.read_parquet(network_pq_path, bbox=layers["XS"].total_bounds)
        network_reach = linemerge(network_reaches.loc[network_reaches["ID"] == int(network_id), "geometry"].iloc[0])
        network_reach_plus_ds_reach = combine_reaches(network_reaches, network_id)

        cm = ConflationMetrics(layers["XS"], layers["River"], network_reach, network_reach_plus_ds_reach)

        metrics = {
            "xs": cm.thalweg_metrics(layers["XS"]),
            "lengths": cm.length_metrics(layers["XS"]),
            "coverage": cm.compute_coverage_metrics(layers["XS"]),
        }
        to_id = network_reaches.loc[network_reaches["ID"] != int(network_id), "to_id"].iloc[0]

        overlapped_reaches = cm.overlapped_reaches(network_reaches[network_reaches["ID"] == int(to_id)])
        eclipsed_reaches = cm.eclipsed_reaches(network_reaches[network_reaches["ID"] != int(network_id)])

        conflation_parameters["reaches"][network_id].update({"metrics": metrics})
        conflation_parameters["reaches"][network_id].update({"overlapped_reaches": overlapped_reaches})
        conflation_parameters["reaches"][network_id].update({"eclipsed_reaches": eclipsed_reaches})
        conflation_parameters["metadata"]["length_units"] = "feet"
        conflation_parameters["metadata"]["flow_units"] = "cfs"

    with open(conflation_json, "w") as f:
        f.write(json.dumps(conflation_parameters, indent=4))
    return conflation_parameters


def combine_reaches(network_reaches: gpd.GeoDataFrame, network_id: str) -> LineString:
    """Combine network reaches."""
    reach = network_reaches.loc[network_reaches["ID"] == int(network_id), :]
    to_reach = network_reaches.loc[network_reaches["ID"] == int(reach["to_id"].iloc[0]), :]
    if to_reach.empty:
        return LineString(linemerge(reach.geometry.iloc[0]).coords)
    else:
        return LineString(
            list(linemerge(reach.geometry.iloc[0]).coords) + list(linemerge(to_reach.geometry.iloc[0]).coords)
        )
