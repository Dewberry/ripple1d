import json

#from ripple.conflation_metrics import RiverConflation
from conflation_metrics import CoverageCalculator

test_reaches = [
    2823972,
    2826228,
    2823932,
    2823960,
    2826230,
    2823934,
    2823920,
    2821866,
    2820012,
    2820006,
    2820002,
]


if __name__ == "__main__":
    conflation_file = r"C:\Users\abiro\OneDrive - Dewberry\Documents\ripple\tests\ras-data\Baxter\Baxter.conflation.json"

    river_id = test_reaches[3]
    try:
        calculator = CoverageCalculator(
            json_data_path = conflation_file,
            parquet_data_path = r"C:\Users\abiro\OneDrive - Dewberry\Documents\ripple\tests\nwm-data\flows.parquet",
            gpkg_data_path= r"C:\Users\abiro\OneDrive - Dewberry\Documents\ripple\tests\ras-data\Baxter\Baxter.gpkg",
            river_id=river_id
        )
        pct_coverage, us_point_gdf, ds_point_gdf, = calculator.calculate_coverage()
        print("conflation results")
        print(f"quantitative coverage: {round(pct_coverage, 2)}%")
        print(f"upstream point: {us_point_gdf['section'].iloc[0]}")
        print(f"downstream point: {ds_point_gdf['section'].iloc[0]}")
    except Exception as e:
        print(f"{river_id}:{e}")