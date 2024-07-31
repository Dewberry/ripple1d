import json
import os

# from ripple.conflation_metrics import RiverConflation
from ripple.conflation_metrics import CoverageCalculator

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
    WK_DIR = os.path.dirname(__file__)
    conflation_file = rf"{WK_DIR}\tests\ras-data\Baxter\Baxter.conflation.json"

    river_id = test_reaches[2]
    try:
        calculator = CoverageCalculator(
            json_data_path=conflation_file,
            parquet_data_path=rf"{WK_DIR}\tests\nwm-data\flows.parquet",
            gpkg_data_path=rf"{WK_DIR}\tests\ras-data\Baxter\Baxter.gpkg",
            river_id=river_id,
        )
        result = calculator.calculate_coverage()

        print(result)
    except Exception as e:
        print(f"{river_id}:{e}")
