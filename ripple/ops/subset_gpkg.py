import json
import logging
import os

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString


def subset_gpkg(
    src_gpkg_path: str,
    ras_project_dir: str,
    nwm_id: str,
    ds_rs: str,
    us_rs: str,
    us_river: str,
    us_reach: str,
    ds_river: str,
    ds_reach: str,
) -> tuple:
    """Subset the cross sections and river geometry for a given NWM reach."""
    # TODO add logic for junctions/multiple river-reaches

    dest_gpkg_path = os.path.join(ras_project_dir, f"{nwm_id}.gpkg")

    # read data
    xs_gdf = gpd.read_file(src_gpkg_path, layer="XS", driver="GPKG")
    river_gdf = gpd.read_file(src_gpkg_path, layer="River", driver="GPKG")
    # if "Junction" in fiona.listlayers(src_gpkg_path):
    #     junction_gdf = gpd.read_file(src_gpkg_path, layer="Junction", driver="GPKG")

    # subset data
    if us_river == ds_river and us_reach == ds_reach:
        xs_subset_gdf = xs_gdf.loc[
            (xs_gdf["river"] == us_river)
            & (xs_gdf["reach"] == us_reach)
            & (xs_gdf["river_station"] >= float(ds_rs))
            & (xs_gdf["river_station"] <= float(us_rs))
        ]

        river_subset_gdf = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
    else:
        xs_us_reach = xs_gdf.loc[
            (xs_gdf["river"] == us_river) & (xs_gdf["reach"] == us_reach) & (xs_gdf["river_station"] <= float(us_rs))
        ]
        xs_ds_reach = xs_gdf.loc[
            (xs_gdf["river"] == ds_river) & (xs_gdf["reach"] == ds_reach) & (xs_gdf["river_station"] >= float(ds_rs))
        ]

        if xs_us_reach["river_station"].min() <= xs_ds_reach["river_station"].max():
            logging.info(
                f"the lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                f" than the highest river station on the downstream reach ({xs_ds_reach['river_station'].max()}) for nwm_id: {nwm_id}"
            )
            xs_us_reach["river_station"] = xs_us_reach["river_station"] + xs_ds_reach["river_station"].max()
            xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                lambda ras_data: update_river_station(ras_data, xs_ds_reach["river_station"].max())
            )

        xs_subset_gdf = pd.concat([xs_us_reach, xs_ds_reach])

        us_reach = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
        ds_reach = river_gdf.loc[(river_gdf["river"] == ds_river) & (river_gdf["reach"] == ds_reach)]
        coords = list(us_reach.iloc[0]["geometry"].coords) + list(ds_reach.iloc[0]["geometry"].coords)
        river_subset_gdf = gpd.GeoDataFrame(
            {"geometry": [LineString(coords)], "river": [nwm_id], "reach": [nwm_id]},
            geometry="geometry",
            crs=us_reach.crs,
        )

    pd.options.mode.copy_on_write = True
    # rename river reach
    xs_subset_gdf["river"] = nwm_id
    xs_subset_gdf["reach"] = nwm_id
    xs_subset_gdf["river_reach"] = f"{nwm_id.ljust(16)},{nwm_id.ljust(16)}"

    river_subset_gdf["river"] = nwm_id
    river_subset_gdf["reach"] = nwm_id
    river_subset_gdf["river_reach"] = f"{nwm_id.ljust(16)},{nwm_id.ljust(16)}"

    # clean river stations
    xs_subset_gdf["ras_data"] = xs_subset_gdf["ras_data"].apply(lambda ras_data: clean_river_stations(ras_data))

    # check if only 1 cross section for nwm_reach
    if len(xs_subset_gdf) <= 1:
        # shutil.rmtree(ras_project_dir)
        logging.warning(f"Only 1 cross section conflated to NWM reach {nwm_id}. Skipping this reach.")
        return None

    # write data
    os.makedirs(ras_project_dir, exist_ok=True)
    xs_subset_gdf.to_file(dest_gpkg_path, layer="XS", driver="GPKG")
    river_subset_gdf.to_file(dest_gpkg_path, layer="River", driver="GPKG")

    return dest_gpkg_path, xs_subset_gdf.crs.to_epsg()


def clean_river_stations(ras_data: str) -> str:
    """Clean up river station data."""
    lines = ras_data.splitlines()
    data = lines[0].split(",")
    data[1] = str(float(lines[0].split(",")[1])).ljust(8)
    lines[0] = ",".join(data)
    return "\n".join(lines) + "\n"


def update_river_station(ras_data: str, river_station: str) -> str:
    """Update river station data."""
    lines = ras_data.splitlines()
    data = lines[0].split(",")
    data[1] = str(float(lines[0].split(",")[1]) + river_station).ljust(8)
    lines[0] = ",".join(data)
    return "\n".join(lines) + "\n"


def junction_length_to_reach_lengths():
    """Adjust reach lengths using junction."""
    # TODO adjust reach lengths using junction lengths
    raise NotImplementedError
    # for row in junction_gdf.iterrows():
    #     if us_river in row["us_rivers"] and us_reach in row["us_reach"]:


def new_gpkg(
    ras_project_directory: str,
    ras_gpkg_file_path: str,
    nwm_id: str,
    ripple_parameters: dict,
    ripple_version,
):
    """Use ripple conflation data to create a new GPKG from an existing ras geopackage."""
    if ripple_parameters["us_xs"]["xs_id"] == "-9999":
        ripple_parameters["messages"] = f"skipping {nwm_id}; no cross sections conflated."
        print(ripple_parameters["messages"])
    else:
        subset_gpkg_path, crs = subset_gpkg(
            ras_gpkg_file_path,
            ras_project_directory,
            nwm_id,
            ripple_parameters["ds_xs"]["xs_id"],
            ripple_parameters["us_xs"]["xs_id"],
            ripple_parameters["us_xs"]["river"],
            ripple_parameters["us_xs"]["reach"],
            ripple_parameters["ds_xs"]["river"],
            ripple_parameters["ds_xs"]["reach"],
        )

        ripple_parameters["files"] = {"gpkg": subset_gpkg_path}
        ripple_parameters["crs"] = crs
        ripple_parameters["version"] = ripple_version

        with open(os.path.join(ras_project_directory, f"{nwm_id}.ripple.json"), "w") as f:
            json.dump({nwm_id: ripple_parameters}, f, indent=4)

    return ripple_parameters


# if __name__ == "__main__":

#     ras_project_directory = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\nwm_models"
#     ras_gpkg_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJMain.gpkg"
#     conflation_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\WFSJMain\WFSJ Main.json"
#     ripple_version = "0.0.1"
#     with open(conflation_json_path) as f:
#         conflation_parameters = json.load(f)

#     ripple_parameters = {}
#     for nwm_id in conflation_parameters.keys():
#         print(f"working on {nwm_id}")
#         ripple_parameters[nwm_id] = new_gpkg(
#             os.path.join(ras_project_directory, nwm_id),
#             ras_gpkg_file_path,
#             nwm_id,
#             conflation_parameters[nwm_id],
#             ripple_version,
#         )
#     with open(conflation_json_path, "w") as f:
#         json.dump(ripple_parameters, f, indent=4)
