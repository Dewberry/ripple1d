import json

#from ripple.conflation_metrics import RiverConflation
from conflation_metrics import RiverConflation

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
    conflation_file = r"/Users/ariellebiro/Dropbox/Mac/Desktop/py_scripts/ripple/tests/ras-data/Baxter/Baxter.conflation.json"
    
    river_id = test_reaches[1]
    try:
        conflation = RiverConflation(
            json_data_path=conflation_file,
            parquet_data_path = r"/Users/ariellebiro/Dropbox/Mac/Desktop/py_scripts/ripple/tests/nwm-data/flows.parquet",
            gpkg_data_path = r"/Users/ariellebiro/Dropbox/Mac/Desktop/py_scripts/ripple/tests/ras-data/Baxter/Baxter.gpkg",
            river_id=river_id
        )
        print(river_id, conflation.generate_conflation_results())
    except Exception as e:
        print(f"{river_id}:{e}")