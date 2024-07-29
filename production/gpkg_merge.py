import os

import geopandas as gpd


def merge_gpkgs(source_dir, model_keys, table_name, output_gpkg, common_crs=None):
    merged_gdf = None

    for model_key in model_keys:
        gpkg_path = os.path.join(source_dir, model_key, f"{model_key}.gpkg")
        if not os.path.exists(gpkg_path):
            print(f"GPKG file for model key {model_key} not found at {gpkg_path}. Skipping.")
            continue

        gdf = gpd.read_file(gpkg_path, layer=table_name)

        if common_crs is not None:
            gdf = gdf.to_crs(common_crs)

        if merged_gdf is None:
            merged_gdf = gdf
        else:
            if common_crs is None and not merged_gdf.crs.equals(gdf.crs):
                gdf = gdf.to_crs(merged_gdf.crs)
            merged_gdf = merged_gdf._append(gdf, ignore_index=True)

    if merged_gdf is not None:
        merged_gdf.to_file(output_gpkg, layer=table_name, driver="GPKG")
        print(f"Successfully merged {table_name} tables into {output_gpkg}.")
    else:
        print(f"No data to merge for table {table_name}.")


if __name__ == "__main__":
    # Example usage
    source_models_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\source_models"
    model_keys = os.listdir(source_models_dir)
    table_name = "XS"  # Can be "XS", "Junction", or "River"
    output_gpkg = r"D:\Users\abdul.siddiqui\workbench\projects\mip_gpkgs\merged_output.gpkg"
    common_crs = "EPSG:4326"  # (e.g., "EPSG:4326") or None to keep original CRSs

    merge_gpkgs(source_models_dir, model_keys, table_name, output_gpkg, common_crs)
