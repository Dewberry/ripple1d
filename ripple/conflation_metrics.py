import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

class GeoProcessor:
    def __init__(self, xs_path, riv_path, test_path, parq_path):
        """Initialize the GeoProcessor with file paths and load the data."""
        self.xs = gpd.read_file(xs_path, layer="XS")
        self.riv = gpd.read_file(riv_path, layer="River")
        self.test = gpd.read_file(test_path)
        self.parq = gpd.read_parquet(parq_path)
        self.filtered_parq = None
        self.total_length_feet = None
        self.intersection_gdf = None
        self.stationing = None
        self.stationing_feet = None
        self.stationing_percentage = None
        self.min_stationing = None
        self.max_stationing = None

    def filter_parquet(self):
        """Filter the parquet file to include only the rows with river IDs present in the test file."""
        self.test['river'] = self.test['river'].astype(int)
        river_ids = self.test['river'].unique()
        self.filtered_parq = self.parq[self.parq['ID'].isin(river_ids)]

    def calculate_lengths(self):
        """Calculate the total length of the NWM reach in feet."""
        lengths = self.filtered_parq.geometry.length
        total_length = lengths.sum()
        self.total_length_feet = total_length * 3.28084

    def ensure_same_crs(self):
        """Ensure that both GeoDataFrames (xs and filtered_parq) are in the same Coordinate Reference System (CRS)."""
        if self.xs.crs != self.filtered_parq.crs:
            self.xs = self.xs.to_crs(self.filtered_parq.crs)

    def calculate_intersections(self):
        """Calculate intersection points and nearest points between xs and filtered_parq geometries."""
        intersection_points = []
        for xs_geom in self.xs.geometry:
            nearest_point = None
            min_distance = float('inf')
            for parq_geom in self.filtered_parq.geometry:
                if xs_geom.intersects(parq_geom):
                    intersection = xs_geom.intersection(parq_geom)
                    if intersection.geom_type == 'Point':
                        intersection_points.append(intersection)
                    elif intersection.geom_type == 'MultiPoint':
                        intersection_points.extend([pt for pt in intersection])
                else:
                    if parq_geom.geom_type == 'Point':
                        nearest_point_on_xs = xs_geom.interpolate(xs_geom.project(parq_geom))
                        distance = nearest_point_on_xs.distance(parq_geom)
                        if distance < min_distance:
                            min_distance = distance
                            nearest_point = nearest_point_on_xs
                    else:
                        parq_point = parq_geom.representative_point()
                        nearest_point_on_xs = xs_geom.interpolate(xs_geom.project(parq_point))
                        distance = nearest_point_on_xs.distance(parq_point)
                        if distance < min_distance:
                            min_distance = distance
                            nearest_point = nearest_point_on_xs
            if nearest_point:
                intersection_points.append(nearest_point)
        self.intersection_gdf = gpd.GeoDataFrame(geometry=intersection_points, crs=self.filtered_parq.crs)

    def compute_stationing(self):
        """Compute the stationing of each intersection point in meters, feet, and as a percentage of the total length."""
        stationing = []
        for point in self.intersection_gdf.geometry:
            nearest_geom = None
            min_distance = float('inf')
            for parq_geom in self.filtered_parq.geometry:
                distance = point.distance(parq_geom)
                if distance < min_distance:
                    min_distance = distance
                    nearest_geom = parq_geom
                if nearest_geom:
                    station = nearest_geom.project(point)
                    stationing.append(station)
        self.stationing = stationing
        self.stationing_feet = [s * 3.28084 for s in stationing]
        self.stationing_percentage = [(s / self.total_length_feet) * 100 for s in self.stationing_feet]
        self.intersection_gdf['stationing_meters'] = stationing
        self.intersection_gdf['stationing_feet'] = self.stationing_feet
        self.intersection_gdf['stationing_percentage'] = self.stationing_percentage
        self.min_stationing = min(self.stationing_feet)
        self.max_stationing = max(self.stationing_feet)

    def generate_result_dict(self):
        """Generate a dictionary containing the total length in feet, downstream minimum stationing, and upstream maximum stationing. Returns a dictionary with keys 'nwm_feet', 'ds_nwm_station', and 'us_nwm_station'."""
        return {
            'nwm_feet': self.total_length_feet,
            'ds_nwm_station': self.min_stationing,
            'us_nwm_station': self.max_stationing
        }

    def process(self):
        """Execute the processing steps in sequence and return the results as a dictionary. dict: A dictionary with keys 'nwm_feet', 'min_stationing', and 'max_stationing'."""
        self.filter_parquet()
        self.calculate_lengths()
        self.ensure_same_crs()
        self.calculate_intersections()
        self.compute_stationing()
        return self.generate_result_dict()

# Usage
geo_processor = GeoProcessor(
xs_path=r"c:\Users\abiro\Downloads\2826228.gpkg",
riv_path=r"c:\Users\abiro\Downloads\2826228.gpkg",
test_path=r"c:\Users\abiro\Downloads\2826228.gpkg",
parq_path=r"C:\Users\abiro\OneDrive - Dewberry\Documents\repo\ripple\tests\nwm-data\flows.parquet"
)
result = geo_processor.process()
print(result)