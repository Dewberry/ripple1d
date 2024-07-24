import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString


class RiverConflation:
    def __init__(self, json_data_path, parquet_data_path, gpkg_data_path, river_id):
        self.json_data_path = json_data_path
        self.parquet_data_path = parquet_data_path
        self.gpkg_data_path = gpkg_data_path
        self.river_id = river_id
        self.read_data()

    def read_data(self):
        """Read data from inputs."""
        self.json_data = pd.read_json(self.json_data_path)
        self.parq = gpd.read_parquet(self.parquet_data_path)
        self.xs = gpd.read_file(self.gpkg_data_path, layer="XS")
        self.river = gpd.read_file(self.gpkg_data_path, layer="River")
        self.matching_instances = self.parq[self.parq["ID"] == self.river_id]

    def calculate_coverage(self) -> float:
        total_length_xs = self.xs.geometry.length.sum()
        total_length_matching_instances = self.matching_instances.geometry.length.sum()
        return float(total_length_matching_instances / total_length_xs)

    def split_and_classify_river(self) -> str:
        river_line = self.river.geometry.union_all()
        centroid = self.matching_instances.geometry.centroid
        distance_from_start = river_line.project(centroid.iloc[0])
        total_length_river = river_line.length
        if distance_from_start < total_length_river / 3:
            return "downstream"
        elif distance_from_start < 2 * total_length_river / 3:
            return "middle"
        else:
            return "upstream"

    def generate_conflation_results(self) -> dict:
        return {
            "nwm_reach_pct_coverage": self.calculate_coverage(),
            "primary_coverage": self.split_and_classify_river(),
        }
