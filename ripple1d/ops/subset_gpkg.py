"""Subset a geopackage based on clonfation with NWM hydrofabric."""

import json
import logging
import os
import warnings
from functools import lru_cache

import fiona
import geopandas as gpd
import pandas as pd
from shapely import LineString
from shapely.ops import split

import ripple1d
from ripple1d.data_model import NwmReachModel, RippleSourceDirectory
from ripple1d.errors import SingleXSModel
from ripple1d.utils.ripple_utils import (
    clip_ras_centerline,
    fix_reversed_xs,
    xs_concave_hull,
)

warnings.filterwarnings("ignore")


class RippleGeopackageSubsetter:
    """Subset a geopackage based on conflation with NWM hydrofabric."""

    def __init__(self, src_gpkg_path: str, conflation_json: str, dst_project_dir: str, nwm_id: str = None):

        self.src_gpkg_path = src_gpkg_path
        self.conflation_json = conflation_json
        self.dst_project_dir = dst_project_dir
        self.nwm_id = nwm_id

    @property
    @lru_cache
    def conflation_parameters(self) -> dict:
        """Extract conflation parameters from the conflation json."""
        with open(self.conflation_json, "r") as f:
            conflation_parameters = json.load(f)
        return conflation_parameters

    @property
    def ripple1d_parameters(self) -> dict:
        """Extract ripple1d parameters from the conflation json."""
        return self.conflation_parameters["reaches"][self.nwm_id]

    @property
    def us_reach(self) -> str:
        """Extract upstream reach from conflation parameters."""
        return self.ripple1d_parameters["us_xs"]["reach"]

    @property
    def us_river(self) -> str:
        """Extract upstream river from conflation parameters."""
        return self.ripple1d_parameters["us_xs"]["river"]

    @property
    def us_rs(self) -> str:
        """Extract upstream river station from conflation parameters."""
        return self.ripple1d_parameters["us_xs"]["xs_id"]

    @property
    def ds_river(self) -> str:
        """Extract downstream river from conflation parameters."""
        return self.ripple1d_parameters["ds_xs"]["river"]

    @property
    def ds_reach(self) -> str:
        """Extract downstream reach from conflation parameters."""
        return self.ripple1d_parameters["ds_xs"]["reach"]

    @property
    def ds_rs(self) -> str:
        """Extract downstream river station from conflation parameters."""
        return self.ripple1d_parameters["ds_xs"]["xs_id"]

    @property
    @lru_cache
    def source_xs(self) -> gpd.GeoDataFrame:
        """Extract cross sections from the source geopackage."""
        xs = gpd.read_file(self.src_gpkg_path, layer="XS")
        source_xs = xs[xs.intersects(self.source_river.union_all())]
        return source_xs

    @property
    @lru_cache
    def source_hulls(self) -> gpd.GeoDataFrame:
        """Extract cross sections from the source geopackage."""
        return gpd.read_file(self.src_gpkg_path, layer="XS_concave_hull")

    @property
    @lru_cache
    def source_river(self) -> gpd.GeoDataFrame:
        """Extract river geometry from the source geopackage."""
        return gpd.read_file(self.src_gpkg_path, layer="River")

    @property
    @lru_cache
    def source_structure(self) -> gpd.GeoDataFrame:
        """Extract structures from the source geopackage."""
        if "Structure" in fiona.listlayers(self.src_gpkg_path):
            structures = gpd.read_file(self.src_gpkg_path, layer="Structure")
            source_structure = structures[structures.intersects(self.source_river.union_all())]
            return source_structure

    @property
    @lru_cache
    def source_junction(self) -> gpd.GeoDataFrame:
        """Extract junctions from the source geopackage."""
        if "Junction" in fiona.listlayers(self.src_gpkg_path):
            source_junction = gpd.read_file(self.src_gpkg_path, layer="Junction")
            return source_junction

    @property
    def ripple_us_xs(self):
        """The most upstream cross section for the ripple model."""
        return self.ripple_xs.loc[
            self.ripple_xs["river_station"] == self.ripple_xs["river_station"].max(), "geometry"
        ].iloc[0]

    @property
    def ripple_ds_xs(self):
        """The most downstream cross section for the ripple model."""
        return self.ripple_xs.loc[
            self.ripple_xs["river_station"] == self.ripple_xs["river_station"].min(), "geometry"
        ].iloc[0]

    @property
    def split_source_hull(self):
        """Split the concave hull of the source model using the upstream and downstream cross sections of the submodel."""
        geoms = split(self.source_hulls.geometry.iloc[0], self.ripple_us_xs).geoms
        hulls = []
        for geom in geoms:
            if geom.intersects(self.ripple_us_xs) and geom.intersects(self.ripple_ds_xs):
                candidate_geoms = split(geom, self.ripple_ds_xs).geoms
                for candidate_geom in candidate_geoms:
                    if candidate_geom.intersects(self.ripple_us_xs) and candidate_geom.intersects(self.ripple_ds_xs):
                        hulls.append(candidate_geom)
        if len(hulls) != 1:
            raise ValueError(
                f"Expected 1 polygon for ripple xs concave hull; got: {len(hulls)} | network id: {self.nwm_id}"
            )
        return hulls

    @property
    @lru_cache
    def ripple_xs_concave_hull(self):
        """Get the concave hull of the cross sections."""
        try:
            hulls = self.split_source_hull
            ripple_xs_concave_hull = gpd.GeoDataFrame({"geometry": hulls}, geometry="geometry", crs=self.crs)
        except Exception as e:
            ripple_xs_concave_hull = xs_concave_hull(fix_reversed_xs(self.ripple_xs, self.ripple_river))
        return ripple_xs_concave_hull

    @property
    def juntion_tree_dict(self) -> dict:
        """Create a dictionary mapping trib->outflow for all junctions."""
        return self.junctions_to_dicts()[0]

    @property
    def juntion_dist_dict(self) -> dict:
        """Create a dictionary mapping trib->outflow for all junctions."""
        return self.junctions_to_dicts()[1]

    @property
    @lru_cache
    def subset_xs(self) -> gpd.GeoDataFrame:
        """Trim source XS to u/s and d/s limits and add all intermediate reaches."""
        subset_xs = pd.DataFrame(data=None, columns=self.source_xs.columns)  # empty copy to put subset into
        subset_xs["source_river_station"] = []
        cur_reach = (self.us_river, self.us_reach)
        ds_reach = (self.ds_river, self.ds_reach)
        _iter = 0
        while True:  # Walk network until d/s reach
            reach_xs = self.source_xs.query(f'river == "{cur_reach[0]}" & reach == "{cur_reach[1]}"')
            reach_xs = self.trim_reach(reach_xs)
            reach_xs["source_river_station"] = reach_xs["river_station"]
            subset_xs["river_station"] += reach_xs["river_station"].max()
            subset_xs = pd.concat([subset_xs, reach_xs]).copy()

            if cur_reach == ds_reach:
                break
            elif _iter > 100 or cur_reach not in self.juntion_tree_dict:
                err_string = "Could not traverse reaches such that u/s river reach led to d/s river reach."
                err_string += "\n"
                err_string += f"Broke on {cur_reach} at {_iter} iterations"
                raise RuntimeError(err_string)
            else:
                _iter += 1
                subset_xs["river_station"] += self.juntion_dist_dict[cur_reach]  # add junction d/s length
                cur_reach = self.juntion_tree_dict[cur_reach]  # move to next d/s reach

        return gpd.GeoDataFrame(subset_xs)

    @property
    @lru_cache
    def subset_river(self) -> gpd.GeoDataFrame:
        """Trim source centerline to u/s and d/s limits and add all intermediate reaches."""
        coords = []
        subset_rivers = []
        for rr in self.subset_xs["river_reach"].unique():
            tmp_river = self.source_river[self.source_river["river_reach"] == rr]
            subset_rivers.append(tmp_river)
            coords.extend(tmp_river.iloc[0]["geometry"].coords)

        subset_rivers = gpd.GeoDataFrame(pd.concat(subset_rivers))
        tmp_line = LineString(coords)
        tmp_xs = fix_reversed_xs(self.subset_xs, subset_rivers)
        tmp_line = clip_ras_centerline(tmp_line, tmp_xs, 2)
        return gpd.GeoDataFrame(
            {"geometry": [tmp_line], "river": [self.nwm_id], "reach": [self.nwm_id]},
            geometry="geometry",
            crs=self.crs,
        )

    @property
    @lru_cache
    def subset_structures(self) -> gpd.GeoDataFrame | None:
        """Extract structures between u/s and d/s limits."""
        if self.source_structure is None:
            return None

        subset_structures = pd.DataFrame(
            data=None, columns=self.source_structure.columns
        )  # empty copy to put subset into
        subset_structures["source_river_station"] = []
        for river_reach in self.subset_xs["river_reach"].unique():
            tmp_xs = self.subset_xs[self.subset_xs["river_reach"] == river_reach]
            us_limit = tmp_xs["source_river_station"].max()  # TODO: can a structure be placed d/s of junction?
            ds_limit = tmp_xs["source_river_station"].min()  # TODO: can a structure be placed u/s of junction?
            tmp_structures = self.source_structure.loc[
                (self.source_structure["river_reach"] == river_reach)
                & (self.source_structure["river_station"] >= float(ds_limit))
                & (self.source_structure["river_station"] <= float(us_limit))
            ]
            tmp_structures["source_river_station"] = tmp_structures["river_station"]
            offset = tmp_xs["source_river_station"].iloc[0] - tmp_xs["river_station"].iloc[0]
            tmp_structures["river_station"] = tmp_structures["river_station"] - offset
            subset_structures = pd.concat([subset_structures, tmp_structures])

        if len(subset_structures) == 0:
            return None
        else:
            return gpd.GeoDataFrame(subset_structures)

    @property
    @lru_cache
    def subset_gdfs(self) -> dict:
        """Subset the cross sections, structues, and river geometry for a given NWM reach."""
        # subset geometry data
        subset_gdfs = {}
        subset_gdfs["XS"] = self.subset_xs
        if len(subset_gdfs["XS"]) <= 1:  # check if only 1 cross section for nwm_reach
            err_string = f"Sub model for {self.nwm_id} would have {len(subset_gdfs['XS'])} cross-sections but is not tagged as eclipsed. Skipping."
            logging.warning(err_string)
            raise SingleXSModel(err_string)
        subset_gdfs["River"] = self.subset_river
        if self.subset_structures is not None:
            subset_gdfs["Structure"] = self.subset_structures

        # Update fields
        for k in subset_gdfs:
            subset_gdfs[k] = self.rename_river_reach(subset_gdfs[k])
        subset_gdfs = self.update_river_station(subset_gdfs)

        return subset_gdfs

    @property
    def ripple_xs(self) -> gpd.GeoDataFrame:
        """Subset cross sections based on NWM reach."""
        return self.subset_gdfs["XS"]

    @property
    def ripple_river(self) -> gpd.GeoDataFrame:
        """Subset river geometry based on NWM reach."""
        return self.subset_gdfs["River"]

    @property
    def ripple_structure(self) -> gpd.GeoDataFrame:
        """Subset structures based on NWM reach."""
        return self.subset_gdfs["Structure"]

    @property
    def ripple_gpkg_file(self) -> str:
        """Return the path to the new geopackage."""
        return self.nwm_reach_model.ras_gpkg_file

    @property
    def nwm_reach_model(self) -> NwmReachModel:
        """Return the new NWM reach model object."""
        return NwmReachModel(self.dst_project_dir)

    @property
    def min_flow(self) -> float:
        """Extract the min flow from the cross sections."""
        if "flows" in self.ripple_xs.columns:
            return self.ripple_xs["flows"].str.split("\n", expand=True).astype(float).min().min()
        else:
            logging.warning(f"no flows specified in source model gpkg for {self.nwm_id}")
            return 10000000000000

    @property
    def max_flow(self) -> float:
        """Extract the max flow from the cross sections."""
        if "flows" in self.ripple_xs.columns:
            return self.ripple_xs["flows"].str.split("\n", expand=True).astype(float).max().max()
        else:
            logging.warning(f"no flows specified in source model gpkg for {self.nwm_id}")
            return 0

    @property
    def crs(self):
        """Extract the CRS from the cross sections."""
        return self.source_xs.crs

    def trim_reach(self, reach_xs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Trim a reach-specific XS gdf to the u/s and d/s limits."""
        # Trim
        river = reach_xs["river"].iloc[0]
        reach = reach_xs["reach"].iloc[0]
        if river == self.us_river and reach == self.us_reach:
            reach_xs = reach_xs[reach_xs["river_station"] <= float(self.us_rs)]
        if river == self.ds_river and reach == self.ds_reach:
            reach_xs = reach_xs[reach_xs["river_station"] >= float(self.ds_rs)]

        return reach_xs

    @lru_cache
    def junctions_to_dicts(self) -> tuple[dict, dict]:
        """Make dicts that map trib->outflow and trib->d/s distance for all junctions."""
        juntion_tree_dict = {}
        juntion_dist_dict = {}

        if self.source_junction is None:
            return (juntion_tree_dict, juntion_dist_dict)

        for r in self.source_junction.iterrows():
            trib_rivers = r[1]["us_rivers"].split(",")
            trib_reaches = r[1]["us_reaches"].split(",")
            trib_dists = r[1]["junction_lengths"].split(",")
            outlet = (r[1]["ds_rivers"], r[1]["ds_reaches"])
            for riv, rch, dist in zip(trib_rivers, trib_reaches, trib_dists):
                juntion_tree_dict[(riv, rch)] = outlet
                juntion_dist_dict[(riv, rch)] = float(dist)

        return (juntion_tree_dict, juntion_dist_dict)

    def write_ripple_gpkg(self) -> None:
        """Write the subsetted geopackage to the destination project directory."""
        os.makedirs(self.dst_project_dir, exist_ok=True)

        for layer, gdf in self.subset_gdfs.items():
            # remove lateral structures
            if layer == "Structure":
                if (gdf["type"] == 6).any():
                    logging.warning(
                        f"Lateral structures are not currently supported in ripple1d. The lateral structures will be dropped."
                    )
                    gdf = gdf.loc[gdf["type"] != 6, :]

            if gdf.shape[0] > 0:
                gdf.to_file(self.ripple_gpkg_file, layer=layer)
                if layer == "XS":
                    self.ripple_xs_concave_hull.to_file(self.ripple_gpkg_file, driver="GPKG", layer="XS_concave_hull")

    def update_river_station(self, subset_gdfs: dict[gpd.GeoDataFrame]) -> dict:
        """Convert river stations to autoincrementing names."""
        xs = subset_gdfs["XS"]
        xs_names = [*range(1, len(xs) + 1)][::-1]

        if "Structure" in subset_gdfs:
            structures = subset_gdfs["Structure"]
            str_names = [xs_names[(xs["river_station"] > i).argmin()] + 0.5 for i in structures["river_station"]]
            subset_gdfs["Structure"]["river_station"] = str_names
            subset_gdfs["Structure"]["ras_data"] = subset_gdfs["Structure"][["ras_data", "river_station"]].apply(
                self.correct_ras_data, axis=1
            )

        subset_gdfs["XS"]["river_station"] = xs_names
        subset_gdfs["XS"]["river_reach_rs"] = (
            subset_gdfs["XS"]["river"]
            + " "
            + subset_gdfs["XS"]["reach"]
            + " "
            + subset_gdfs["XS"]["river_station"].astype(str)
        )
        subset_gdfs["XS"]["ras_data"] = subset_gdfs["XS"][["ras_data", "river_station"]].apply(
            self.correct_ras_data, axis=1
        )

        return subset_gdfs

    def correct_ras_data(self, row):
        """Make ras_data names consistent with river_station."""
        ras_data = row["ras_data"]
        rs = row["river_station"]

        lines = ras_data.splitlines()
        data = lines[0].split(",")
        if "*" in data[1]:
            data[1] = str(float(rs)) + "*"
            data[1] = data[1].ljust(8)
        else:
            data[1] = str(float(rs)).ljust(8)
        lines[0] = ",".join(data)
        return "\n".join(lines) + "\n"

    def rename_river_reach(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Rename river, reach, and river_reach columns after the nwm reach id."""
        pd.options.mode.copy_on_write = True
        gdf["river"] = self.nwm_id
        gdf["reach"] = self.nwm_id
        gdf["river_reach"] = f"{self.nwm_id.ljust(16)},{self.nwm_id.ljust(16)}"

        return gdf

    def update_ripple1d_parameters(self, rsd: RippleSourceDirectory):
        """Update ripple1d_parameters with results of subsetting."""
        ripple1d_parameters = self.ripple1d_parameters
        ripple1d_parameters["source_model"] = rsd.ras_project_file
        ripple1d_parameters["crs"] = self.crs.to_wkt()
        ripple1d_parameters["version"] = ripple1d.__version__
        ripple1d_parameters["high_flow"] = max([ripple1d_parameters["high_flow"], self.max_flow])
        ripple1d_parameters["low_flow"] = min([ripple1d_parameters["low_flow"], self.min_flow])
        if ripple1d_parameters["high_flow"] == self.max_flow:
            ripple1d_parameters["notes"] = ["high_flow computed from source model flows"]
        if ripple1d_parameters["low_flow"] == self.min_flow:
            ripple1d_parameters["notes"] = ["low_flow computed from source model flows"]
        return ripple1d_parameters

    def write_ripple1d_parameters(self, ripple1d_parameters: dict):
        """Write ripple1d parameters to json file."""
        with open(os.path.join(self.dst_project_dir, f"{self.nwm_id}.ripple1d.json"), "w") as f:
            json.dump(ripple1d_parameters, f, indent=4)


def extract_submodel(source_model_directory: str, submodel_directory: str, nwm_id: int):
    """Use ripple conflation data to create a new GPKG from an existing ras geopackage.

    Create a new geopackage with information for a specific NWM reach.  The new geopackage contains layer for the river centerline, cross-sections, and structures.

    Parameters
    ----------
    source_model_directory : str
        The path to the directory containing HEC-RAS project, plan, geometry, and flow files.
    submodel_directory : str
        The path to export submodel HEC-RAS files to.
    nwm_id : int
        The id of the NWM reach to create a submodel for
    task_id : str, optional
        Task ID to use for logging, by default ""

    Returns
    -------
    dict
        Metadata for the submodel

    Raises
    ------
    FileNotFoundError
        Raised when no geopackage is found in the source model directory
    FileNotFoundError
        Raised when no .conflation.json is found in the source model directory
    """
    if not os.path.exists(source_model_directory):
        raise FileNotFoundError(
            f"cannot find directory for source model {source_model_directory}, please ensure dir exists"
        )
    rsd = RippleSourceDirectory(source_model_directory)

    logging.info(f"extract_submodel starting for nwm_id {nwm_id}")

    if not rsd.file_exists(rsd.ras_gpkg_file):
        raise FileNotFoundError(f"cannot find file ras-geometry file {rsd.ras_gpkg_file}, please ensure file exists")

    if not rsd.file_exists(rsd.conflation_file):
        raise FileNotFoundError(f"cannot find conflation file {rsd.conflation_file}, please ensure file exists")

    ripple1d_parameters = rsd.nwm_conflation_parameters(str(nwm_id))
    if ripple1d_parameters["eclipsed"]:
        ripple1d_parameters["messages"] = f"skipping {nwm_id}; no cross sections conflated."
        logging.warning(ripple1d_parameters["messages"])
        gpkg_path = None
        conflation_file = None

    else:
        rgs = RippleGeopackageSubsetter(rsd.ras_gpkg_file, rsd.conflation_file, submodel_directory, nwm_id)
        rgs.write_ripple_gpkg()
        ripple1d_parameters = rgs.update_ripple1d_parameters(rsd)
        rgs.write_ripple1d_parameters(ripple1d_parameters)
        gpkg_path = rgs.ripple_gpkg_file
        conflation_file = rsd.conflation_file

    logging.info(f"extract_submodel complete for nwm_id {nwm_id}")
    return {"ripple1d_parameters": conflation_file, "ripple_gpkg_file": gpkg_path}
