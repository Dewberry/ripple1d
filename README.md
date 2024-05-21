# ripple
Utilities for reuse of HEC-RAS models for NWM. HEC-RAS models must be cataloged in a Spatial Temporal Asset Catalog (STAC) and conflated with the NWM reach dataset prior to processing through ripple. 

## Contents
- /conflate: RAS-FIM Conflation
- /stacio: Build and update STAC items
- /exe: Build RAS Terrains and add to STAC items
- /examples: examples


## Getting Started
To utilize this repository to develop flood inundations maps from 1D steady-state HEC-RAS models the following are required:
- A Windows opertating system
- HEC-RAS 6.3.1 installed
- Python packages specified in requirements-windows.txt installed
- Access to s3
- An href for a STAC item containing the following minimum content:
    - Assets representing the necessary HEC-RAS files with role assigned as "RAS-FILE"
        - HEC-RAS project file
        - HEC-RAS geometry file 
        - HEC-RAS plan file
        - HEC-RAS flow file

    - Assets for HEC-RAS terrain data with role assigned as "RAS-TOPO"
        - RAS_Terrain.hdf
        - RAS_Terrain.tif
        - RAS_Terrain.vrt 

    - An asset for a geopackage containing the model geometry with role "RAS-GEOMETRY-GPKG"

    - An asset containing conflation parameters with assigned role "RIPPLE-PARAMS".
        The conflation parameters asset should be a nested json which contains a key for each NWM branch associated with RAS model. The parameters shown below must exists for each NWM branch.

        ![alt text](image-2.png)    



## About
Producing inundation maps at half-foot increments for each NWM branch in a given RAS model is a multi-step process outlined below. "run_process.py" is a script that executes the process in the necessary sequential order. 

1. Read the input STAC item
2. Download HEC-RAS files
3. Load NWM conflation parameters.
2. Detemine flow increments for each NWM branch in the RAS model using upper and lower flow ranges specified in conflation parameters. Default is 10 increments.
3. For each NWM branch, write a flow and plan file for an initial run using the incremented flows. 
4. For each NWM branch, Use the results from the initial run to develop rating curves at the upstream and downstream terminus of each NWM branch. 
5. For the upstream rating-curve of each branch, determine what flows would be needed to prouduce half-foot increments.
6. For the downstream rating-curve of each branch, determine depths at half-foot increments over the range of flows applied to the branch. 
7. Convert the downstream depths to water surface elevations
8. For each NWM branch create a production-run flow/plan file. 
    - The discharges derived from the upstream rating-curve are applied at the top of the HEC-RAS river-reach
    - The water surface elevations derived from the downstream rating-curve are applied as an intermediate known water surface elevation at the downstream terminus of the NWM branch
9. Run the production-runs with post-processing depth rasters toggled on
10. For each NWM branch, clip each resulting depth raster to a concave hull derieved from the cross sections associated with each branch.
11. For each NWM branch read the HDF results and write rating-curves to sqlite db.  