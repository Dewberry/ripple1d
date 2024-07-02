# Production Scripts
The scripts contained in this "Production" directory are example scripts that run all of the processes necessary to leverage FEMA MIP models to develop FIM libraries using ripple. Each script/process is outlined below. 
## Step_1_extract_geometry.py
Extract the geometry of an existing HEC-RAS model and create a geopackage. Currently, this script reads from an s3 location but ripple does have built in capacity to read from local. 

The discharges applied to the existing model are also extracted.

Structures are not currently supported. 

## Step_2_create_stac.py
Create a Spatial-Temporal Asset Catalog (STAC) item for an existing HEC-RAS model. Use the geopackage from step 1 to define the spatial location of the model. 

## Step_3_conflate_model.py
Conflate an existing HEC-RAS model with the NWM reaches. Outputs a json of conflation parameters.

## Step_4_subset_gpkg.py
Subset a geopackage representing an existing HEC-RAS model's geometry based on the results of conflating with the NWM reaches. Multiple new geopackages are typically produced; each one corresponding to a single NWM reach. 

## Step_5_create_terrain.py
Create a HEC-RAS terrain for each NWM-based geopackage. The convex hull of the geopackage's cross section layer is buffered and then used to clip a source DEM.   

## Step_6_initial_normal_depth.py
Write a HEC-RAS model for each NWM-based geopackage, HEC-RAS terrain, and conflation parameters.
Create a flow and plan for an initial normal depth run. The discharges applied are derived from the min/max flow from the NWM for this reach and the min/max flows pulled from the existing HEC-RAS model. 10 discharges are incremented evenly between the smallest and largest flows from the two aforementioned sources.

This run is computed to develop a rating curve which will inform incremental discharges that produce depth increments provided by the user in the next step. 

Compute the initial normal depth plan.

## Step_7_incremental_normal_depth.py
Based on rating curve derived from the initial-normal-depth-run and a target depth increment provided by the user, create a second normal depth run where the discharges are read from the rating curve at the specified depth increments.

Compute the incremental normal depth plan.

## Step_8_known_water_surface_elevation.py
Create a new flow/plan for a known water surface elevation (kwse) run. This run uses the range of flows applied for the incremental-normal-depth-run and a min elevation, max elevation, and depth increment provded by the user to simulated all flow-kwse scenarios. The provided min/max elevations should represent the min/max water surface elevation expected at the downstream end of the NWM reach.

For each flow-kwse combination, the kwse is compared to the water surface elevation resulting from the normal depth run whose flow is the same as the current flow. If the kwse is lower than the water surface elevation from the normal depth run, then the kwse will not control downstream portion of the model and thus the flow-kwse combination is removed from the list of necessary simulations.   

## Step_9_create_fim_lib.py
Create a library of depth grids resulting from the known-water-surface-elevation-runs and the incremental-normal-depth-runs. 

A database is also produced which contains the flows and kwse that were applied as well as the computed water surface elevations and depths at the upstrema end of NWM reach.
