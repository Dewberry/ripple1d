import json

from .process import subset_gpkg


def main(ras_project_directory: str, ras_gpkg_file_path: str, nwm_id: str, nwm_data: dict):

    subset_gpkg_path = subset_gpkg(
        ras_gpkg_file_path,
        ras_project_directory,
        nwm_id,
        nwm_data["ds_xs"]["xs_id"],
        nwm_data["us_xs"]["xs_id"],
        nwm_data["us_xs"]["river"],
        nwm_data["us_xs"]["reach"],
        nwm_data["ds_xs"]["river"],
        nwm_data["ds_xs"]["reach"],
    )
    print(subset_gpkg_path)


if __name__ == "__main__":

    ras_project_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\test"
    ras_gpkg_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\Baxter.gpkg"
    json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\Baxter\baxter-ripple-params.json"

    with open(json_path) as f:
        ripple_parameters = json.load(f)

    for nwm_id, nwm_data in ripple_parameters.items():
        main(ras_project_directory, ras_gpkg_file_path, nwm_id, nwm_data)
