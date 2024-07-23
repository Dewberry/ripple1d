import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

class RiverConflation:
    def __init__(self, json_data_path, parquet_data_path, gpkg_data_path, river_id):
        self.json_data_path = json_data_path
        self.parquet_data_path = parquet_data_path
        self.gpkg_data_path = gpkg_data_path
        self.river_id = river_id
        self.read_data()
    
    def read_data(self):
        self.json_data = pd.read_json(self.json_data_path)
        self.parq = gpd.read_parquet(self.parquet_data_path)
        self.xs = gpd.read_file(self.gpkg_data_path, layer="XS")
        self.river = gpd.read_file(self.gpkg_data_path, layer="River")
        self.matching_instances = self.parq[self.parq['ID'] == self.river_id]
    
    def calculate_coverage(self):
        total_length_xs = self.xs.geometry.length.sum()
        total_length_matching_instances = self.matching_instances.geometry.length.sum()
        self.coverage_percentage = (total_length_matching_instances / total_length_xs) * 100
    
    def split_and_classify_river(self):
        river_line = self.river.geometry.union_all()
        centroid = self.matching_instances.geometry.centroid
        distance_from_start = river_line.project(centroid.iloc[0])
        total_length_river = river_line.length
        if distance_from_start < total_length_river / 3:
            self.classification = "downstream"
        elif distance_from_start < 2 * total_length_river / 3:
            self.classification = "middle"
        else:
            self.classification = "upstream"
    
    def generate_conflation_results(self):
        print("Conflation Results:")
        print(f"Quantitative Coverage: {round(self.coverage_percentage, 1)}%")
        print(f"Section of River Covered: {self.classification.capitalize()}")

# Example usage
conflation = RiverConflation(
    json_data_path=r'C:\Users\abiro\Downloads\BASIN FORK Trib 4-nwm_conflation (1).json',
    parquet_data_path=r'C:\Users\abiro\Downloads\nwm_flows_v3 (1).parquet',
    gpkg_data_path=r'C:\Users\abiro\Downloads\BASIN FORK Trib 4.gpkg',
    river_id=5998592
)
conflation.calculate_coverage()
conflation.split_and_classify_river()
conflation.generate_conflation_results()
