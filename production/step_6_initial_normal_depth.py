import json

from ripple.ops.run_ras_model import initial_normal_depth

if __name__ == "__main__":
    conflation_json_path = (
        r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"
    )
    with open(conflation_json_path) as f:
        conflation_parameters = json.load(f)

    for nwm_id in conflation_parameters.keys():
        print(nwm_id)
        new_ras_project_text_file = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.prj"
        )

        subset_gpkg_path = (
            rf"C:\Users\mdeshotel\Downloads\12040101_Models\ripple2\ripple\tests\ras-data\Baxter\{nwm_id}\{nwm_id}.gpkg"
        )
        initial_normal_depth(
            nwm_id,
            f"{nwm_id}_ind",
            conflation_parameters[nwm_id],
            new_ras_project_text_file,
            subset_gpkg_path,
        )
