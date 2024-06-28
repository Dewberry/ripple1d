import logging
import os
from datetime import datetime
from pathlib import Path

import pystac
import pystac_client
import pystac_client.errors

from ripple.utils.s3_utils import check_s3_key_exists
from ripple.utils.stac_utils import (
    key_to_uri,
    upsert_collection,
    upsert_item,
    uri_to_key,
)


class FIMCollection:
    """Class for interacting with a FIM collection in a STAC API."""

    def __init__(self, stac_api: str, collection_id: str, connect: bool = True) -> str:
        self.stac_api = stac_api
        self._collection_id = collection_id
        if connect:
            self.collection = self.load()
        else:
            self.collection = None

    def __repr__(self) -> str:
        """Return a string representation of the FIMCollection."""
        return f"FIMCollection: {self._collection_id}"

    def load(self) -> pystac.Collection:
        """Load the collection from the STAC API."""
        try:
            client = pystac_client.Client.open(self.stac_api)
        except Exception as e:
            raise ConnectionError(f"Error loading STAC API: {self.stac_api}: {e}")

        try:
            return client.get_collection(self._collection_id)
        except Exception:
            raise KeyError(f"Collection `{self._collection_id}` does not exist. Use new_collection() to create.")

    def add_branch_conflation_asset(
        self,
        asset_id: str,
        asset_key: str,
        roles: str = pystac.MediaType.GEOPACKAGE,
        bucket: str = "fim",
    ):
        """Add a branch conflation asset to the collection."""
        asset_url = key_to_uri(asset_key, bucket)
        asset = pystac.Asset(
            href=asset_url,
            description="FIM Branches and Control Nodes",
            roles=[roles],
            extra_fields={"s3_key": asset_key, "updated": datetime.now().isoformat()},
        )
        self.collection.add_asset(asset_id, asset)

    def post_collection_updates(self):
        """Post collection updates to the STAC API."""
        return upsert_collection(self.stac_api, self.collection)

    # def new_item(self, item: pystac.Item):
    #     self.collection.add_item(item)
    #     self.item = item


class FIMCollectionRasItem(FIMCollection):
    """Class for interacting with a FIM collection RAS item in a STAC API."""

    def __init__(
        self,
        stac_api: str,
        collection_id: str,
        item_id: str,
        connect: bool = False,
        load: bool = True,
        local_item: pystac.Item = None,
    ):
        super().__init__(stac_api, collection_id, connect)

        if load:
            if isinstance(local_item, pystac.Item):
                self.item = local_item
                self._item_id = self.item.id
            else:
                self._item_id = item_id
                self.item = self.load_item()
        else:
            raise KeyError(f"Item `{self._item_id}` does not exist. Use new_item() to create.")

    def __repr__(self) -> str:
        """Return a string representation of the FIMCollectionRasItem."""
        return f"FIMCollectionRasItem: {self._collection_id}-{self._item_id}"

    def load_item(self) -> pystac.Item:
        """Load the item from the STAC API."""
        try:
            return self.collection.get_item(self._item_id)
        except Exception:
            raise KeyError(f"Item `{self._item_id}` does not exist. Use new_item() to create.")

    # def add_topo_assets(self, topo_filename: str = "MapTerrain"):

    def post_item_updates(self):
        """Post item updates to the STAC API."""
        return upsert_item(self.stac_api, self.collection.id, self.item)

    def sanitize_ras_stac_props(self):
        """
        TODO: Placeholder function for sanitizing RAS STAC properties.

        Need to update ras-stac and remove this method here or convert to a check.
        """
        # Grab the gpkg location (href)
        for asset_name in self.item.get_assets(role=pystac.MediaType.GEOPACKAGE):
            asset = self.item.assets[asset_name]
            try:
                asset.roles.remove("data")
            except Exception:
                logging.warning(f"no data role: {asset.href}")

            if "ras-geometry-gpkg" not in asset.roles:
                asset.roles.append("ras-geometry-gpkg")

        for asset_name in self.item.get_assets(role="forcing-file"):
            asset = self.item.assets[asset_name]
            try:
                asset.roles.append("hec-ras")
            except Exception:
                logging.warning(f"no data role: {asset.href}")

        for asset_name in self.item.get_assets(role="geometry-file"):
            asset = self.item.assets[asset_name]
            try:
                asset.roles.append("hec-ras")
            except Exception:
                logging.warning(f"no data role: {asset.href}")

        for asset_name in self.item.get_assets(role="plan-file"):
            asset = self.item.assets[asset_name]
            try:
                asset.roles.append("hec-ras")
            except Exception:
                logging.warning(f"no data role: {asset.href}")

        for asset_name in self.item.get_assets(role="projection"):
            asset = self.item.assets[asset_name]
            try:
                asset.roles.remove("projection")
                asset.roles.append("hec-ras")
                asset.roles.append("project-file")
            except Exception:
                logging.warning(f"no data role: {asset.href}")

    def add_s3_key_to_assets(self, bucket: str = "fim"):
        """Add the s3_key to the assets in the item."""
        for asset_name in self.item.assets:
            asset = self.item.assets[asset_name]
            asset.extra_fields["s3_key"] = uri_to_key(asset.href, bucket)

    def ensure_asset_roles_unique(self, bucket: str = "fim"):
        """Ensure that the asset roles are unique."""
        for asset_name in self.item.assets:
            asset = self.item.assets[asset_name]
            asset.roles = list(set(asset.roles))

    def map_topo_assets(
        self,
        date_aquired: str = datetime.now().isoformat(),
        software: str = "ripple v0.1.0-alpha.1",
        topo_filename: str = "MapTerrain",
        asset_role: str = "ras-geometry-gpkg",
        source: str = "USGS_Seamless_DEM_13",
    ):
        """
        TODO: Placeholder function for mapping topo assets to FIM collection items.

        This assumes the topo assets are in the same directory as the `ras-geometry-gpkg` asset
        """
        if source != "USGS_Seamless_DEM_13":
            raise NotImplementedError("Only USGS_Seamless_DEM_13 is supported at this time.")

        if asset_role != "ras-geometry-gpkg":
            raise NotImplementedError("Only ras-geometry-gpkg is supported at this time.")

        if topo_filename != "MapTerrain":
            raise NotImplementedError("Only MapTerrain is supported at this time.")

        for asset_name in self.item.get_assets(role=asset_role):
            # logging.debug(asset_name)
            asset_href = self.item.assets[asset_name].href

        topo_href = str(Path(asset_href).parent / "MapTerrain/MapTerrain.ned13.tif").replace("https:/", "https://")
        ras_topo_href = str(Path(asset_href).parent / "MapTerrain/MapTerrain.hdf").replace("https:/", "https://")
        vrt_href = str(Path(asset_href).parent / "MapTerrain/MapTerrain.vrt").replace("https:/", "https://")
        topo_source = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"

        self.item.add_asset(
            f"{topo_filename}.tif",
            pystac.Asset(
                topo_href,
                title=f"{topo_filename}.tif",
                roles=[pystac.MediaType.COG, "ras-topo"],
                extra_fields={
                    "topo_source_url": topo_source,
                    "date_aquired": date_aquired,
                    "software": software,
                },
            ),
        )

        # Add ras topo asset to item
        self.item.add_asset(
            f"{topo_filename}.hdf",
            pystac.Asset(
                ras_topo_href,
                title=f"{topo_filename}.hdf",
                roles=[pystac.MediaType.HDF, "ras-topo"],
                extra_fields={
                    "software": software,
                    "date_aquired": date_aquired,
                },
            ),
        )

        # Add ras topo asset to item
        self.item.add_asset(
            f"{topo_filename}.vrt",
            pystac.Asset(
                vrt_href,
                title=f"{topo_filename}.vrt",
                roles=[pystac.MediaType.XML, "ras-topo"],
                extra_fields={
                    "software": software,
                    "date_aquired": date_aquired,
                },
            ),
        )

    def add_ripple_params(
        self,
        date_created: str = datetime.now().isoformat(),
        software: str = "ripple v0.1.0-alpha.1",
        asset_role: str = "project-file",
        bucket: str = "fim",
    ) -> bool:
        """
        TODO: Placeholder function for adding ripple-params to FIM collection items.

        This assumes the conflation output is in the same directory as the `project-file` asset
        """
        if asset_role != "project-file":
            raise NotImplementedError("Only project-file is supported at this time.")

        for asset_name in self.item.get_assets(role=asset_role):
            asset_href = self.item.assets[asset_name].href

        ripple_parameters_href = str((Path(asset_href).parent / "ripple_parameters.json")).replace(
            "https:/", "https://"
        )
        ripple_parameters_key = uri_to_key(ripple_parameters_href, bucket)

        # logging.debug(ripple_parameters_key)
        if check_s3_key_exists(bucket, ripple_parameters_key):
            self.item.add_asset(
                "ripple_parameters.json",
                pystac.Asset(
                    ripple_parameters_href,
                    title="ConflationParameters",
                    roles=[pystac.MediaType.JSON, "ripple-params"],
                    extra_fields={
                        "software": software,
                        "date_created": date_created,
                    },
                ),
            )
            return True
        else:
            return False


class FIMCollectionRasDGItem(FIMCollection):
    """
    Class for interacting with a FIM collection RAS Depth Grid item in a STAC API.

    Not Implemented
    """

    def __init__(self, stac_api: str, collection_id: str, item_id: str, load: bool = True):
        super().__init__(stac_api, collection_id, load)
        raise NotImplementedError("FIMCollectionRasDGItem is not implemented.")
        self._item_id = item_id
        if load:
            self.item = self.load_item()
        else:
            raise KeyError(f"Item `{self._item_id}` does not exist. Use new_item() to create.")

    def __repr__(self) -> str:
        """Return a string representation of the FIMCollectionRasDGItem."""
        return f"FIMCollectionDGItem: {self._collection_id}-{self._item_id}"

    def load_item(self) -> pystac.Item:
        """Load the item from the STAC API."""
        try:
            return self.collection.get_item(self._item_id)
        except Exception:
            raise KeyError(f"Item `{self._item_id}` does not exist. Use new_item() to create.")
