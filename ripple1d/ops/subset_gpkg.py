"""Subset a geopackage based on clonfation with NWM hydrofabric."""

import json
import logging
import os

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString


def subset_gpkg(
    src_gpkg_path: str,
    dst_project_dir: str,
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

    # read data
    xs_gdf = gpd.read_file(src_gpkg_path, layer="XS", driver="GPKG")
    river_gdf = gpd.read_file(src_gpkg_path, layer="River", driver="GPKG")
    structures_gdf = None
    if "Structure" in fiona.listlayers(src_gpkg_path):
        structures_gdf = gpd.read_file(src_gpkg_path, layer="Structure", driver="GPKG")
    if "Junction" in fiona.listlayers(src_gpkg_path):
        junction_gdf = gpd.read_file(src_gpkg_path, layer="Junction", driver="GPKG")

    # subset data
    if us_river == ds_river and us_reach == ds_reach:
        xs_subset_gdf = xs_gdf.loc[
            (xs_gdf["river"] == us_river)
            & (xs_gdf["reach"] == us_reach)
            & (xs_gdf["river_station"] >= float(ds_rs))
            & (xs_gdf["river_station"] <= float(us_rs))
        ]
        if structures_gdf is not None:
            structures_subset_gdf = structures_gdf.loc[
                (structures_gdf["river"] == us_river)
                & (structures_gdf["reach"] == us_reach)
                & (structures_gdf["river_station"] >= float(ds_rs))
                & (structures_gdf["river_station"] <= float(us_rs))
            ]
        river_subset_gdf = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
    else:
        if "Junction" in fiona.listlayers(src_gpkg_path):
            junction_gdf = gpd.read_file(src_gpkg_path, layer="Junction", driver="GPKG")
            xs_intermediate_river_reaches = walk_junctions(junction_gdf, us_river, us_reach, ds_river, ds_reach)

        xs_us_reach = xs_gdf.loc[
            (xs_gdf["river"] == us_river) & (xs_gdf["reach"] == us_reach) & (xs_gdf["river_station"] <= float(us_rs))
        ]
        xs_ds_reach = xs_gdf.loc[
            (xs_gdf["river"] == ds_river) & (xs_gdf["reach"] == ds_reach) & (xs_gdf["river_station"] >= float(ds_rs))
        ]
        if structures_gdf is not None:
            structures_us_reach = structures_gdf.loc[
                (structures_gdf["river"] == us_river)
                & (structures_gdf["reach"] == us_reach)
                & (structures_gdf["river_station"] <= float(us_rs))
            ]
            structures_ds_reach = structures_gdf.loc[
                (structures_gdf["river"] == ds_river)
                & (structures_gdf["reach"] == ds_reach)
                & (structures_gdf["river_station"] >= float(ds_rs))
            ]

        if xs_intermediate_river_reaches:
            for xs_intermediate_river_reach in xs_intermediate_river_reaches:
                xs_intermediate_river_reach = xs_gdf.loc[xs_gdf["river_reach"] == xs_intermediate_river_reach]
                if xs_us_reach["river_station"].min() <= xs_intermediate_river_reach["river_station"].max():
                    logging.warning(
                        f"the lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                        f" than the highest river station on the intermediate reach ({xs_intermediate_river_reach['river_station'].max()}) for nwm_id: {nwm_id}"
                    )
                    xs_us_reach["river_station"] = (
                        xs_us_reach["river_station"] + xs_intermediate_river_reach["river_station"].max()
                    )
                    xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                        lambda ras_data: update_river_station(
                            ras_data, xs_intermediate_river_reach["river_station"].max()
                        )
                    )

                xs_us_reach = pd.concat([xs_us_reach, xs_intermediate_river_reach])

        if xs_us_reach["river_station"].min() <= xs_ds_reach["river_station"].max():
            logging.warning(
                f"the lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                f" than the highest river station on the downstream reach ({xs_ds_reach['river_station'].max()}) for nwm_id: {nwm_id}"
            )
            xs_us_reach["river_station"] = xs_us_reach["river_station"] + xs_ds_reach["river_station"].max()
            xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                lambda ras_data: update_river_station(ras_data, xs_ds_reach["river_station"].max())
            )
            if structures_gdf is not None:
                structures_us_reach["river_station"] = (
                    structures_us_reach["river_station"] + structures_ds_reach["river_station"].max()
                )
                structures_us_reach["ras_data"] = structures_us_reach["ras_data"].apply(
                    lambda ras_data: update_river_station(ras_data, structures_ds_reach["river_station"].max())
                )

        xs_subset_gdf = pd.concat([xs_us_reach, xs_ds_reach])
        if structures_gdf is not None:
            structures_subset_gdf = pd.concat([structures_us_reach, structures_ds_reach])

        us_reach = river_gdf.loc[(river_gdf["river"] == us_river) & (river_gdf["reach"] == us_reach)]
        ds_reach = river_gdf.loc[(river_gdf["river"] == ds_river) & (river_gdf["reach"] == ds_reach)]

        # handle river reach coords
        coords = list(us_reach.iloc[0]["geometry"].coords)
        if xs_intermediate_river_reaches:
            for xs_intermediate_river_reach in xs_intermediate_river_reaches:
                intermediate_river_reach = river_gdf.loc[river_gdf["river_reach"] == xs_intermediate_river_reach]
                coords += list(intermediate_river_reach.iloc[0]["geometry"].coords)
        coords += list(ds_reach.iloc[0]["geometry"].coords)

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

    if structures_gdf is not None:
        structures_subset_gdf["river"] = nwm_id
        structures_subset_gdf["reach"] = nwm_id
        structures_subset_gdf["river_reach"] = f"{nwm_id.ljust(16)},{nwm_id.ljust(16)}"

    river_subset_gdf["river"] = nwm_id
    river_subset_gdf["reach"] = nwm_id
    river_subset_gdf["river_reach"] = f"{nwm_id.ljust(16)},{nwm_id.ljust(16)}"

    # clean river stations
    xs_subset_gdf["ras_data"] = xs_subset_gdf["ras_data"].apply(lambda ras_data: clean_river_stations(ras_data))
    if structures_gdf is not None:
        structures_subset_gdf["ras_data"] = structures_subset_gdf["ras_data"].apply(
            lambda ras_data: clean_river_stations(ras_data)
        )
    xs_subset_gdf["river_station"] = xs_subset_gdf["river_station"].round().astype(float)

    # check if only 1 cross section for nwm_reach
    if len(xs_subset_gdf) <= 1:
        # shutil.rmtree(ras_project_dir)
        logging.warning(f"Only 1 cross section conflated to NWM reach {nwm_id}. Skipping this reach.")
        return None

    # write data
    new_nwm_reach_model = NwmReachModel(dst_project_dir)
    os.makedirs(dst_project_dir, exist_ok=True)
    xs_subset_gdf.to_file(new_nwm_reach_model.ras_gpkg_file, layer="XS", driver="GPKG")
    if structures_gdf is not None and len(structures_subset_gdf) > 0:
        structures_subset_gdf.to_file(new_nwm_reach_model.ras_gpkg_file, layer="Structure", driver="GPKG")

    # clip river to cross sections
    crs = xs_subset_gdf.crs

    buffer = 10
    while True:
        concave_hull = xs_concave_hull(xs_subset_gdf).to_crs(epsg=5070).buffer(buffer * METERS_PER_FOOT).to_crs(crs)
        clipped_river_subset_gdf = river_subset_gdf.clip(concave_hull)
        clipped_river_subset_gdf.to_file(new_nwm_reach_model.ras_gpkg_file, layer="River", driver="GPKG")
        buffer += 10
        if len(clipped_river_subset_gdf) == 1 & xs_subset_gdf.intersects(
            clipped_river_subset_gdf["geometry"].iloc[0]
        ).all() & isinstance(clipped_river_subset_gdf["geometry"].iloc[0], LineString):
            break
        if buffer > 10000:
            raise ValueError(
                f"buffer too large (>10000ft) for clipping river to concave hull of cross sections. Check data for NWM Reach: {nwm_id}."
            )
            break

    if "flows" in xs_subset_gdf.columns:
        max_flow = xs_subset_gdf["flows"].str.split("\n", expand=True).astype(float).max().max()
        min_flow = xs_subset_gdf["flows"].str.split("\n", expand=True).astype(float).min().min()
    else:
        min_flow, max_flow = 10000000000000, 0
        logging.warning(f"no flows specified in source model gpkg for {nwm_id}")

    return new_nwm_reach_model.ras_gpkg_file, xs_subset_gdf.crs.to_epsg(), max_flow, min_flow


def walk_junctions(junction_gdf, us_river, us_reach, ds_river, ds_reach) -> tuple:
    """Check if junctions are present for the given river-reaches."""
    river_reaches = []
    if not junction_gdf.empty:
        while True:
            for _, row in junction_gdf.iterrows():

                us_rivers = row.us_rivers.split(",")
                us_reaches = row.us_reaches.split(",")

                for river, reach in zip(us_rivers, us_reaches):
                    if row.ds_rivers == ds_river and row.ds_reaches == ds_reach:
                        return river_reaches
                    if river == us_river and reach == us_reach:
                        river_reaches.append(f"{row.ds_rivers.ljust(16)},{row.ds_reaches.ljust(16)}")
                        us_river = row.ds_rivers
                        us_reach = row.ds_reaches


def clean_river_stations(ras_data: str) -> str:
    """Clean up river station data."""
    lines = ras_data.splitlines()
    data = lines[0].split(",")
    data[1] = str(float(round(float(lines[0].split(",")[1])))).ljust(8)
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


def extract_submodel(
    source_model_directory: str,
    submodel_directory: str,
    nwm_id: int,
    ripple_version: str = RIPPLE_VERSION,
):
    """Use ripple conflation data to create a new GPKG from an existing ras geopackage."""
    rsm = RippleSourceModel(source_model_directory)
    logging.info(f"Preparing to extract NWM ID {nwm_id} from {rsm.ras_project_file}")

    if not rsm.file_exists(rsm.ras_gpkg_file):
        raise FileNotFoundError(f"cannot find file ras-geometry file {rsm.ras_gpkg_file}, please ensure file exists")

    if not rsm.file_exists(rsm.conflation_file):
        raise FileNotFoundError(f"cannot find conflation file {rsm.conflation_file}, please ensure file exists")

    ripple_parameters = rsm.nwm_conflation_parameters(str(nwm_id))
    if ripple_parameters["us_xs"]["xs_id"] == "-9999":
        ripple_parameters["messages"] = f"skipping {nwm_id}; no cross sections conflated."
        logging.warning(ripple_parameters["messages"])

    else:
        subset_gpkg_path, crs, max_flow, min_flow = subset_gpkg(
            rsm.ras_gpkg_file,
            submodel_directory,
            nwm_id,
            ripple_parameters["ds_xs"]["xs_id"],
            ripple_parameters["us_xs"]["xs_id"],
            ripple_parameters["us_xs"]["river"],
            ripple_parameters["us_xs"]["reach"],
            ripple_parameters["ds_xs"]["river"],
            ripple_parameters["ds_xs"]["reach"],
        )
        ripple_parameters["source_model"] = rsm.ras_project_file
        ripple_parameters["crs"] = crs
        ripple_parameters["version"] = ripple_version
        ripple_parameters["high_flow_cfs"] = max([ripple_parameters["high_flow_cfs"], max_flow])
        ripple_parameters["low_flow_cfs"] = min([ripple_parameters["low_flow_cfs"], min_flow])
        if ripple_parameters["high_flow_cfs"] == max_flow:
            ripple_parameters["notes"] = ["high_flow_cfs computed from source model flows"]
        if ripple_parameters["low_flow_cfs"] == min_flow:
            ripple_parameters["notes"] = ["low_flow_cfs computed from source model flows"]

        with open(os.path.join(submodel_directory, f"{nwm_id}.ripple1d.json"), "w") as f:
            json.dump(ripple_parameters, f, indent=4)

    return ripple_parameters
