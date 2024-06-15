import pandas as pd

from ripple.ras import RasGeomText, RasManager

geom_file1 = r"C:\Users\mdeshotel\Downloads\12040101_Models\Hydraulic Models_HarveyUpdate\West Fork San Jacinto River\WFSJR 055\WFSJR 055.g01"
geom_file2 = r"C:\Users\mdeshotel\Downloads\12040101_Models\Hydraulic Models_HarveyUpdate\West Fork San Jacinto River\WFSJ Main\WFSJ Main.g01"
geom_file3 = r"C:\Users\mdeshotel\Downloads\12040101_Models\Hydraulic Models_HarveyUpdate\Crystal Creek-West Fork San Jacinto River\STEWARTS CREEK\STEWARTS CREEK.g01"
projection = 'PROJCS["NAD_1983_StatePlane_Texas_Central_FIPS_4203_Feet",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",2296583.333],PARAMETER["False_Northing",9842500.0],PARAMETER["Central_Meridian",-100.333333333333],PARAMETER["Standard_Parallel_1",31.8833333333333],PARAMETER["Standard_Parallel_2",30.1166666666667],PARAMETER["Latitude_Of_Origin",29.6666666666667],UNIT["US survey foot",0.304800609601219]]'
combined_gpkg = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\combined\combined.gpkg"
ras_project_text_file = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\tests\ras-data\combined1\combined.prj"

geom1 = RasGeomText(geom_file1, projection)
geom2 = RasGeomText(geom_file2, projection)
geom3 = RasGeomText(geom_file3, projection)
xs_gdf = pd.concat([geom1.xs_gdf, geom2.xs_gdf, geom3.xs_gdf])
reach_gdf = pd.concat([geom1.reach_gdf, geom2.reach_gdf, geom3.reach_gdf])
xs_gdf.to_file(combined_gpkg, layer="XS")
reach_gdf.to_file(combined_gpkg, layer="River")


rm=RasManager.from_gpkg(ras_project_text_file, "combined", combined_gpkg, version="631")
