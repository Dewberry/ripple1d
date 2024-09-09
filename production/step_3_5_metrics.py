from ripple1d.ops.metrics import compute_conflation_metrics
from ripple1d.ripple1d_logger import configure_logging

if __name__ == "__main__":
    TEST_DIR = os.path.dirname(__file__).replace("production", "tests")

    src_gpkg_path = os.path.join(TEST_DIR, "ras-data\\Baxter\\Baxter.gpkg")
    conflation_json = os.path.join(TEST_DIR, "ras-data\\Baxter\\Baxter.conflation.json")
    nwm_pq_path = os.path.join(TEST_DIR, "nwm-data\\flows.parquet")
    conflation_parameters = compute_conflation_metrics(src_gpkg_path, nwm_pq_path, conflation_json)
