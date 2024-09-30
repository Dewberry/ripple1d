"""Subset a geopackage based on clonfation with NWM hydrofabric."""

import json
import logging
import os

import fiona
import geopandas as gpd
import pandas as pd
from shapely import LineString
from shapely.ops import split

import ripple1d
from ripple1d.consts import METERS_PER_FOOT
from ripple1d.data_model import NwmReachModel, RippleSourceDirectory, RippleSourceModel
from ripple1d.ripple1d_logger import log_process
from ripple1d.utils.ripple_utils import clip_ras_centerline, fix_reversed_xs, xs_concave_hull


class RippleGeopackageSubsetter:
    """Subset a geopackage based on conflation with NWM hydrofabric."""

    def __init__(self, src_gpkg_path: str, conflation_json: str, dst_project_dir: str, nwm_id: str = None):

        self.src_gpkg_path = src_gpkg_path
        self.conflation_json = conflation_json
        self.dst_project_dir = dst_project_dir
        self.nwm_id = nwm_id
        self._subset_gdf = None
        self._source_junction = None
        self._source_river = None
        self._source_xs = None
        self._source_structure = None
        self._conflation_parameters = None
        self._source_hulls = None
        self._ripple_xs_concave_hull = None

    def set_nwm_id(self, nwm_id: str):
        self.nwm_id = nwm_id
        self._subset_gdf = None
        self._ripple_xs_concave_hull = None

    @property
    def ripple_us_xs(self):
        return self.ripple_xs.loc[
            self.ripple_xs["river_station"] == self.ripple_xs["river_station"].max(), "geometry"
        ].iloc[0]

    @property
    def ripple_ds_xs(self):
        return self.ripple_xs.loc[
            self.ripple_xs["river_station"] == self.ripple_xs["river_station"].min(), "geometry"
        ].iloc[0]

    @property
    def split_source_hull(self):
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
    def ripple_xs_concave_hull(self):
        if self._ripple_xs_concave_hull is None:
            try:
                hulls = self.split_source_hull
                self._ripple_xs_concave_hull = gpd.GeoDataFrame({"geometry": hulls}, geometry="geometry", crs=self.crs)
            except Exception as e:
                self._ripple_xs_concave_hull = xs_concave_hull(fix_reversed_xs(self.ripple_xs, self.ripple_river))

        return self._ripple_xs_concave_hull

    @property
    def conflation_parameters(self) -> dict:
        """Extract conflation parameters from the conflation json."""
        if self._conflation_parameters is None:
            with open(self.conflation_json, "r") as f:
                self._conflation_parameters = json.load(f)
        return self._conflation_parameters

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
    def source_xs(self) -> gpd.GeoDataFrame:
        """Extract cross sections from the source geopackage."""
        if self._source_xs is None:
            xs = gpd.read_file(self.src_gpkg_path, layer="XS")
            self._source_xs = xs[xs.intersects(self.source_river.union_all())]
        return self._source_xs

    @property
    def source_hulls(self) -> gpd.GeoDataFrame:
        """Extract cross sections from the source geopackage."""
        if self._source_hulls is None:
            # TODO we can read the concave hull of the source model from the gpkg once we decide that it is required layer.
            # self._source_hulls = gpd.read_file(self.src_gpkg_path, layer="XS_concave_hull")
            self._source_hulls = xs_concave_hull(
                fix_reversed_xs(self.source_xs, self.source_river), self.source_junction
            )
        return self._source_hulls

    @property
    def source_river(self) -> gpd.GeoDataFrame:
        """Extract river geometry from the source geopackage."""
        if self._source_river is None:
            self._source_river = gpd.read_file(self.src_gpkg_path, layer="River")
        return self._source_river

    @property
    def source_structure(self) -> gpd.GeoDataFrame:
        """Extract structures from the source geopackage."""
        if "Structure" in fiona.listlayers(self.src_gpkg_path):
            if self._source_structure is None:
                structures = gpd.read_file(self.src_gpkg_path, layer="Structure")
                self._source_structure = structures[structures.intersects(self.source_river.union_all())]
            return self._source_structure

    @property
    def source_junction(self) -> gpd.GeoDataFrame:
        """Extract junctions from the source geopackage."""
        if "Junction" in fiona.listlayers(self.src_gpkg_path):
            if self._source_junction is None:
                self._source_junction = gpd.read_file(self.src_gpkg_path, layer="Junction")
            return self._source_junction

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
    def subset_gdfs(self) -> dict:
        """Subset the cross sections, structues, and river geometry for a given NWM reach."""
        if self._subset_gdf is None:
            # subset data
            if self.us_river == self.ds_river and self.us_reach == self.ds_reach:
                ripple_xs, ripple_structure, ripple_river = self.process_as_one_ras_reach()
            else:
                ripple_xs, ripple_structure, ripple_river = self.process_as_multiple_ras_reach()

            # check if only 1 cross section for nwm_reach
            if len(ripple_xs) <= 1:
                logging.warning(f"Only 1 cross section conflated to NWM reach {self.nwm_id}. Skipping this reach.")
                return None

            # update fields
            ripple_xs = self.update_fields(ripple_xs)
            ripple_river = self.update_fields(ripple_river)
            ripple_structure = self.update_fields(ripple_structure)

            # clip river to cross sections
            ripple_river["geometry"] = clip_ras_centerline(
                ripple_river.iloc[0].geometry, fix_reversed_xs(ripple_xs, ripple_river), 2
            )

            if ripple_structure is not None and len(ripple_structure) > 0:
                self._subset_gdf = {
                    "XS": ripple_xs,
                    "River": ripple_river,
                    "Structure": ripple_structure,
                }
            else:
                self._subset_gdf = {
                    "XS": ripple_xs,
                    "River": ripple_river,
                }

        return self._subset_gdf

    @property
    def ripple_gpkg_file(self) -> str:
        """Return the path to the new geopackage."""
        return self.nwm_reach_model.ras_gpkg_file

    @property
    def nwm_reach_model(self) -> NwmReachModel:
        """Return the new NWM reach model object."""
        return NwmReachModel(self.dst_project_dir)

    @log_process
    def write_ripple_gpkg(self, task_id: str = None) -> None:
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

    def walk_junctions(self) -> list[str]:
        """Check if junctions are present for the given river-reaches."""
        river_reaches = []
        if not self.source_junction.empty:
            c = 0
            while True:
                c += 1
                if c > 100:
                    logging.warning(
                        f"Could not find junction for: {self.nwm_id}. The reach may contain cross sections from multiple RAS reaches that are not connected via junctions."
                    )
                    return None

                for _, row in self.source_junction.iterrows():

                    us_rivers = row.us_rivers.split(",")
                    us_reaches = row.us_reaches.split(",")

                    for river, reach in zip(us_rivers, us_reaches):
                        if row.ds_rivers == self.ds_river and row.ds_reaches == self.ds_reach:
                            return river_reaches
                        if river == self.us_river and reach == self.us_reach:
                            river_reaches.append(f"{row.ds_rivers.ljust(16)},{row.ds_reaches.ljust(16)}")
                            us_river = row.ds_rivers
                            us_reach = row.ds_reaches

    def clean_river_stations(self, ras_data: str) -> str:
        """Clean up river station data."""
        lines = ras_data.splitlines()
        data = lines[0].split(",")
        if "*" in data[1]:
            data[1] = str(float(data[1].rstrip("*"))) + "*"
            data[1] = data[1].ljust(8)
        else:
            data[1] = str(float(data[1])).ljust(8)
        lines[0] = ",".join(data)
        return "\n".join(lines) + "\n"

    def round_river_stations(self, ras_data: str) -> str:
        """Clean up river station data."""
        lines = ras_data.splitlines()
        data = lines[0].split(",")
        if "*" in data[1]:
            data[1] = str(float(round(float(data[1].rstrip("*"))))) + "*"
            data[1] = data[1].ljust(8)
        else:
            data[1] = str(float(round(float(data[1])))).ljust(8)
        lines[0] = ",".join(data)
        return "\n".join(lines) + "\n"

    def update_river_station(self, ras_data: str, river_station: str) -> str:
        """Update river station data."""
        lines = ras_data.splitlines()
        data = lines[0].split(",")
        if "*" in data[1]:
            data[1] = str(float(data[1].rstrip("*")) + river_station) + "*"
            data[1] = data[1].ljust(8)
        else:
            data[1] = str(float(data[1]) + river_station).ljust(8)
        lines[0] = ",".join(data)
        return "\n".join(lines) + "\n"

    def junction_length_to_reach_lengths(self):
        """Adjust reach lengths using junction."""
        # TODO adjust reach lengths using junction lengths
        raise NotImplementedError
        # for row in junction_gdf.iterrows():
        #     if us_river in row["us_rivers"] and us_reach in row["us_reach"]:

    def process_as_one_ras_reach(self) -> tuple:
        """Process as a single ras-river-reach."""
        xs_subset_gdf = self.source_xs.loc[
            (self.source_xs["river"] == self.us_river)
            & (self.source_xs["reach"] == self.us_reach)
            & (self.source_xs["river_station"] >= float(self.ds_rs))
            & (self.source_xs["river_station"] <= float(self.us_rs))
        ]
        if self.source_structure is not None:
            structures_subset_gdf = self.source_structure.loc[
                (self.source_structure["river"] == self.us_river)
                & (self.source_structure["reach"] == self.us_reach)
                & (self.source_structure["river_station"] >= float(self.ds_rs))
                & (self.source_structure["river_station"] <= float(self.us_rs))
            ]
        else:
            structures_subset_gdf = None
        river_subset_gdf = self.source_river.loc[
            (self.source_river["river"] == self.us_river) & (self.source_river["reach"] == self.us_reach)
        ]

        return xs_subset_gdf, structures_subset_gdf, river_subset_gdf

    @property
    def xs_us_reach(self) -> gpd.GeoDataFrame:
        """Extract cross sections for the upstream reach."""
        return self.source_xs.loc[
            (self.source_xs["river"] == self.us_river)
            & (self.source_xs["reach"] == self.us_reach)
            & (self.source_xs["river_station"] <= float(self.us_rs))
        ]

    @property
    def xs_ds_reach(self) -> gpd.GeoDataFrame:
        """Extract cross sections for the downstream reach."""
        return self.source_xs.loc[
            (self.source_xs["river"] == self.ds_river)
            & (self.source_xs["reach"] == self.ds_reach)
            & (self.source_xs["river_station"] >= float(self.ds_rs))
        ]

    @property
    def structures_us_reach(self) -> gpd.GeoDataFrame:
        """Extract structures for the upstream reach."""
        if self.source_structure is not None:
            return self.source_structure.loc[
                (self.source_structure["river"] == self.us_river)
                & (self.source_structure["reach"] == self.us_reach)
                & (self.source_structure["river_station"] <= float(self.us_rs))
            ]

    @property
    def structures_ds_reach(self) -> gpd.GeoDataFrame:
        """Extract structures for the downstream reach."""
        if self.source_structure is not None:
            return self.source_structure.loc[
                (self.source_structure["river"] == self.ds_river)
                & (self.source_structure["reach"] == self.ds_reach)
                & (self.source_structure["river_station"] >= float(self.ds_rs))
            ]

    def add_intermediate_river_reaches(self, intermediate_river_reaches) -> gpd.GeoDataFrame:
        """Add intermediate river reaches to the xs_us_reach."""
        xs_us_reach = self.xs_us_reach.copy()
        for intermediate_river_reach in intermediate_river_reaches:
            xs_intermediate_river_reach = self.source_xs.loc[self.source_xs["river_reach"] == intermediate_river_reach]
            if xs_us_reach["river_station"].min() <= xs_intermediate_river_reach["river_station"].max():
                logging.info(
                    f"The lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                    f" than the highest river station on the intermediate reach"
                    f"({xs_intermediate_river_reach['river_station'].max()}) for nwm_id: {self.nwm_id}. The river"
                    f" stationing of the upstream reach will be updated to ensure river stationings increase from"
                    "downstream to upstream"
                )
                xs_us_reach["river_station"] = (
                    xs_us_reach["river_station"] + xs_intermediate_river_reach["river_station"].max()
                )
                xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                    lambda ras_data: self.update_river_station(
                        ras_data, xs_intermediate_river_reach["river_station"].max()
                    )
                )
            xs_us_reach = pd.concat([xs_us_reach, xs_intermediate_river_reach])
        return xs_us_reach

    def adjust_river_stations(self, xs_us_reach, structures_us_reach) -> tuple:
        """Adjust river stations of the upstream reach if the min river station of the upstream reach is less than the max river station of the downstream reach."""
        if xs_us_reach["river_station"].min() <= self.xs_ds_reach["river_station"].max():
            logging.info(
                f"the lowest river station on the upstream reach ({xs_us_reach['river_station'].min()}) is less"
                f" than the highest river station on the downstream reach ({self.xs_ds_reach['river_station'].max()}) for nwm_id: {self.nwm_id}"
            )
            xs_us_reach["river_station"] = xs_us_reach["river_station"] + self.xs_ds_reach["river_station"].max()
            xs_us_reach["ras_data"] = xs_us_reach["ras_data"].apply(
                lambda ras_data: self.update_river_station(ras_data, self.xs_ds_reach["river_station"].max())
            )
            if structures_us_reach is not None:
                structures_us_reach["river_station"] = (
                    structures_us_reach["river_station"] + self.xs_ds_reach["river_station"].max()
                )
                structures_us_reach["ras_data"] = structures_us_reach["ras_data"].apply(
                    lambda ras_data: self.update_river_station(ras_data, self.xs_ds_reach["river_station"].max())
                )
        return xs_us_reach, structures_us_reach

    def process_as_multiple_ras_reach(self) -> tuple:
        """Process as multiple ras-river-reach."""
        if "Junction" in fiona.listlayers(self.src_gpkg_path):
            junction_gdf = gpd.read_file(self.src_gpkg_path, layer="Junction")
            intermediate_river_reaches = self.walk_junctions()
        else:
            intermediate_river_reaches = None

        # add intermediate river reaches to the upstream reach
        if intermediate_river_reaches:
            xs_us_reach = self.add_intermediate_river_reaches(intermediate_river_reaches)
        else:
            xs_us_reach = self.xs_us_reach.copy()

        # update river stations
        xs_us_reach, structures_us_reach = self.adjust_river_stations(xs_us_reach, self.structures_us_reach)
        # combine us and ds gdfs
        xs_subset_gdf = pd.concat([xs_us_reach, self.xs_ds_reach])
        river_subset_gdf = self.combine_reach_features(intermediate_river_reaches)

        if self.source_structure is not None:
            structures_subset_gdf = pd.concat([structures_us_reach, self.structures_ds_reach])
        else:
            structures_subset_gdf = None

        return xs_subset_gdf, structures_subset_gdf, river_subset_gdf

    def rename_river_reach(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Rename river, reach, and river_reach columns after the nwm reach id."""
        pd.options.mode.copy_on_write = True
        gdf["river"] = self.nwm_id
        gdf["reach"] = self.nwm_id
        gdf["river_reach"] = f"{self.nwm_id.ljust(16)},{self.nwm_id.ljust(16)}"

        return gdf

    def combine_reach_features(self, intermediate_river_reaches: list[str]) -> gpd.GeoDataFrame:
        """Combine reach coordinates and update river and reach names to be nwm id."""
        us_reach = self.source_river.loc[
            (self.source_river["river"] == self.us_river) & (self.source_river["reach"] == self.us_reach)
        ]
        ds_reach = self.source_river.loc[
            (self.source_river["river"] == self.ds_river) & (self.source_river["reach"] == self.ds_reach)
        ]

        # handle river reach coords
        coords = list(us_reach.iloc[0]["geometry"].coords)
        if intermediate_river_reaches:
            for intermediate_river_reach in intermediate_river_reaches:
                intermediate_river_reach_reach = self.source_river.loc[
                    self.source_river["river_reach"] == intermediate_river_reach
                ]
                coords += list(intermediate_river_reach_reach.iloc[0]["geometry"].coords)
        coords += list(ds_reach.iloc[0]["geometry"].coords)

        return gpd.GeoDataFrame(
            {"geometry": [LineString(coords)], "river": [self.nwm_id], "reach": [self.nwm_id]},
            geometry="geometry",
            crs=self.crs,
        )

    def update_fields(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Update fields for the new NWM reach."""
        if gdf is not None:
            gdf = self.rename_river_reach(gdf)

            # clean river stations
            if "river_station" in gdf.columns:
                if (gdf["river_station"].astype(str).str.len() > 8).any():
                    gdf["ras_data"] = gdf["ras_data"].apply(lambda ras_data: self.round_river_stations(ras_data))
                    gdf["river_station"] = gdf["river_station"].round().astype(float)
                else:
                    gdf["ras_data"] = gdf["ras_data"].apply(lambda ras_data: self.clean_river_stations(ras_data))
                    gdf["river_station"] = gdf["river_station"].astype(float)
        return gdf

    def update_ripple1d_parameters(self, rsd: RippleSourceDirectory):
        """Update ripple1d_parameters with results of subsetting."""
        ripple1d_parameters = self.ripple1d_parameters
        ripple1d_parameters["source_model"] = rsd.ras_project_file
        ripple1d_parameters["crs"] = self.crs.to_epsg()
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


def extract_submodel(source_model_directory: str, submodel_directory: str, nwm_id: int, task_id: str = ""):
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
    logging.info(f"{task_id} | extract_submodel starting")
    rsd = RippleSourceDirectory(source_model_directory)
    logging.debug(f"{task_id} | preparing to extract NWM ID {nwm_id} from {rsd.ras_project_file}")

    if not rsd.file_exists(rsd.ras_gpkg_file):
        raise FileNotFoundError(
            f"{task_id} | cannot find file ras-geometry file {rsd.ras_gpkg_file}, please ensure file exists"
        )

    if not rsd.file_exists(rsd.conflation_file):
        raise FileNotFoundError(
            f"{task_id} | cannot find conflation file {rsd.conflation_file}, please ensure file exists"
        )

    ripple1d_parameters = rsd.nwm_conflation_parameters(str(nwm_id))
    if ripple1d_parameters["eclipsed"]:
        ripple1d_parameters["messages"] = f"skipping {nwm_id}; no cross sections conflated."
        logging.warning(ripple1d_parameters["messages"])

    else:
        rgs = RippleGeopackageSubsetter(rsd.ras_gpkg_file, rsd.conflation_file, submodel_directory, nwm_id)
        rgs.write_ripple_gpkg(task_id=task_id)
        ripple1d_parameters = rgs.update_ripple1d_parameters(rsd)
        rgs.write_ripple1d_parameters(ripple1d_parameters)

    logging.info(f"{task_id} | extract_submodel complete")
    return ripple1d_parameters
