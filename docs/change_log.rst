Change Log
==========

.. note::
   Go to the `Releases <https://github.com/Dewberry/ripple1d/releases.html>`__  page for a list of all releases.

Feature Release 0.7.0
~~~~~~~~~~~~~~~~~~~~~
Users Changelog
----------------

This release of `ripple1d` attempts to speed up the known water surface elevation (kwse) computes by parallelizing the creation of the depth grids. To do this, all kwse runs are ran without producing depth grids. From these initial kwse runs the rating curve database are created and used to inform the boundary condition for the next reach upstream. Once all of the boundary conditions are known, a second kwse run is ran to produce the depth grids in parallel. To make this happen some changes were necessary to the endpoints.

In addition to adding/modifying the endpoints, this release of `ripple1d` makes significant updates to both ripple1d execution and logging. This includes running each endpoint as a separate subprocess which allows the user to have the ability to dismiss running jobs. This should be handy for when jobs appear to have hung up. Dismissing these hung up jobs will free up cpu for new jobs. 

Features Added
----------------
**Write depth grids argument**
A new argument boolean "write_depth_grids" was added to the "run_incremental_normal_depth" and "run_known_wse" endpoints. This allows the user to specify whether ripple1d should compute raw RAS depth grids or not.

**Create rating curves database**
A new endpoint called "create_rating_curves_db" was added. This endpoint is a post processing step that creates a rating curve database from RAS plan hdf results. This endpoint only requires 2 args: "submodel_directory" and "plans. The location of the rating curve database is inferred from the submodel directory. It will be located directly in the submodel directory and will be named as the network reach name; e.g., "2820002/2820002.db".

The "create_rating_curves_db" endpoint checks for the presence of the depth grid for each profile and records its existence or absence along with the plan suffix in the database table. The columns are "plan_suffix" and "map_exist" and are of type string and boolean, respectively.

**Create FIM library update**
The "create_fim_lib" endpoint no longer produces the rating curve database.

**Update to the Ripple1d workflow**
The user should take care with the args when calling the new/modified endpoints; specifically the plan names. The recommended order for calling these endpoints is:

1. run_known_wse : with "write_depth_grids" set to false and "plan_suffix" set to ["ikwse"]
2. create_rating_curves_db: with "plans" set to ["ikwse"]
3. run_known_wse: with "write_depth_grids" set to true and "plan_suffix" set to ["nd","kwse"]
4. create_rating_curves_db: with "plans" set to ["nd","kwse"]
5. create_fim_lib: with "plans" set to ["nd","kwse"]

**Subprocess encapsulation**
The execution of all endpoints are now encapsulated within subprocesses

**Logs and database**

- Huey and flask logs have been combined into a single log file (server-logs.jsonld).
- Huey.db has been renamed to jobs.db
- The process id for each endpoint are now tracked in a p_id field of the task_status table in jobs.db
- A huey_status field has been added to the task_status table. This field tracks the execution status of the endpoint subprocess.
- A new table called task_logs has been added to jobs.db. This table contains stdout, stderr, and results stemming from endpoint subprocesses.
- A proof of concept graphical (html-based) job status page has been added


Bugfix Release 0.6.3
~~~~~~~~~~~~~~~~~~~~~
 
Users Changelog
----------------
This release of `ripple1d` fixes several bugs identified during testing.

Bug Fixes
----------
- Technical Documentation has been updated with high level summary of package functionality.
- The ID column was removed from geopackage layers. All code dependencies on the ID column have been removed. 
- Now only reaches that are connected via the "to_id" are considered eclipsed reaches.
- Precision has been added to the rating curves used to inform the incremental normal depth runs by reducing the amount of rounding.
- CRS is now stored as WKT instead of EPSG in the ripple.json file to more robustly represent all possible CRSs; e.g., ESRI.  

Bugfix Release 0.6.2
~~~~~~~~~~~~~~~~~~~~~

Users Changelog
----------------
This release of `ripple1d` fixes several bugs associated with conflation. To aid in identifying and fixing these bugs, improvements were made in the logging for the conflation endpoint. In summary, the fixes and changes incorporated in this PR improve the consistency and quality of the conflation process, computations, and metrics in genera with special attention for handling junctions and headwater reaches.


Bugfix Release 0.6.1
~~~~~~~~~~~~~~~~~~~~~

Users Changelog
----------------
This release of `ripple1d` fixes several bugs identified during testing.

Features Added
----------------
- A minor change was added to the logging behavior to improve error tracking. 

Bug Fixes
----------
- A bug causing increasing processing time when calling `creat_ras_terrain` in parallel mode.
- A bug in the `extract_submodel` endpoint which failed when trying to grab the upstream cross section. A check was added for the eclipsed parameter, where if true no geopackage will be created. 
- Several bugs associated with the `create_fim_lib endpoint`: 

  1. The library_directory arg was not being implemented correctly in the function. 
  2. If a fim.db already exists append fuctionality has been implemented.
  3. If the directory containing the raw RAS depth grids is empty the clean up function will not be invoked.
- Resolves issues introduced when a concave hull from a source model where cross section existed in the wrong direction (resulting in a multipart polygon). A check was added to correct direction and reverses the cross section if it was drawn incorrectly. This is limited to the development of the concave hull and does not modify the cross section direction for use in the modeling. 

Feature Release 0.6.0
~~~~~~~~~~~~~~~~~~~~~
Users Changelog
----------------

This release of `ripple1d` adds 2 args to the create_fim_lib endpoint, adds a concave hull of the cross sections to the geopackage, and fixes a bug associated with the depth grids.

Features Added
----------------
**New library directory argument**

A new required arg, "library_directory", has been added to the create_fim_lib endpoint. This new arg specifies where the output directory for the FIM grids and database. 

**New cleanup argument**

A new required arg, "cleanup", has been added to the create_fim_lib endpoint. If this arg is True the raw HEC-RAS depth grids are deleted. If False they are not deleted.

**Concave hull of cross sections**

A new layer representing the concave hull of the cross sections has been added to the geopackage for the source model and the network based model. It also improves how the concave hull is handled at junctions by explicitly creating a junction concave hull and then merging it in with the xs concave hull.


Bug Fixes
----------------

- An error was arising when all normal depth runs resulted in water surface elevations that were below the mapping terrain which means no resulting depth grids were being produced. Previously the code assumed at least 1 depth grid would be present. This has been fixed by obtaining the "terrain part" of the raw RAS grid from the RAS terrain instead of the first depth grid in the raw RAS result folder.


Feature Release 0.5.0
~~~~~~~~~~~~~~~~~~~~~
Users Changelog
----------------

This release of `ripple1d` incorporates geometry extraction, conflation, and conflation metrics into the API, and fixes several bugs.
 
 
Features Added
----------------
**Conflation improvements**

- The source HEC-RAS river centerline is now clipped to the most upstream and most downstream cross sections prior to starting conflation. This helps prevent identifying network reaches that are far away from the cross sections and improves the accuracy of the conflation.  
- Overlapped reaches are now tracked and documented in the conflation json file.
- A bbox column has been added to the network parquet file for faster reading. This was especially needed for the new conflation endpoint since each request needs to load the parquet file. Load times without the bbox column were between 5-20 seconds; this is reduced to 1-2 seconds with the bbox column. 
- The conflation function now reads locally instead of from s3.
- The conflation function no longer creates a STAC item.
- RAS metadata is now added to the conflation json. 
- The source network's metadata is now added to the conflation json.
- Length and flow units are now documented in the conflation json file.

**Conflation Metrics**

Three metrics are computed to asses the qualitiy of the conflation:

- `Coverage`: The the start and end location of the reach coverage is computed as a ratio of the length of the network reach.
- `Lengths`: The lengths between the most upstream cross section and most downstream cross section along the network reach and source HEC-RAS Model's centerline is computed. The ratio of the two lengths is also provided.
- `XS`: The distance between where the network reach and HEC-RAS Model's centerline intersects the cross sections is computed. A similar comparison is performed using the cross section's thalweg location and the network reaches intersection location with the cross sections. The mean, min, max, std, and quartiles are provided as a summary for both comparisons.  
 
 
**Geometry Extraction improvements**
- A new function to verify .prj file is a HEC-RAS file has been added.
- The extracted geopackage now contians a non-spatial metadata table for the souce HEC-RAS model. 
- Tests have been added for extracting geopackage from HEC-RAS model.
- Additional attributes are added to the source model gpkg for downstream use. 
- Units are extracted from the source RAS model and added to metadata.

**API**

- An endpoint was added for extracting geometry and relevant metdata for the soure HEC-RAS models and storing it in in a geopackage. 
- An endpoint to compute conflation metric was added.
- An endpoint for conflation (which includes metrics calculations) was added.
- Tests were added for the conflation, conflation metrics, and geopackage endpoints.
 
 
Bug Fixes
----------

- Reaches whose conflation results indicate upstream and downstream cross sections are the same are now considered a failed conflation. 
- The function to create a concave hull for the cross sections has been improved when junctions are present. 
- Eclipsed reaches are now better identified and are documented in the conflation json with a boolean. 
- A check is now performed to ensure cross sections intersect the source HEC-RAS model's river centerline. If cross sections do not intersect the centerline they are dropped. 
- A conflation json is no longer written for source HEC-RAS models that fail to conflate. 
- Handling has been added to subset gpkg endpoint for river stationings of interpolated. These river stations contain an "*" to indicate interpolated cross section.
- Several issues with the automated API tests were identified and fixed. 
- API tests no longer re-run gpkg_from_ras and conflate_model for every reach; just once per source test model. 
- When API tests pass the resulting files are now removed automatically. Resulting files for tests that fail are not removed so that the tester can better trouble shoot.
  


Bugfix Release 0.4.1-0.4.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Users Changelog
----------------
This release of `ripple1d` fixes several bugs identified during testing.

Features Added
----------------
No features added in this version.

**API**
- `ripple_version` is no longer an option for the body of any endpoints.

Bug Fixes
----------
- A bug due to a hard coded terrain path name causing an error on  `create_fim_lib` has been resolved.
- A bug associated with the `ripple_version` parameter has been resolved by removing the parameter from the body of requests (see note in API above).
- An issue with including lateral structures (not yet implemented) in the ras geometry files causing hang ups  has been resolved. This fix resolved another issue where stationing was mis-applied in the newly created ras geometry files.
- A bug which caused a failure when calling subset_gpkg in cases where the model geometries are simple (no structures / no junctions).


Feature Release 0.4.0
~~~~~~~~~~~~~~~~~~~~~


Users Changelog
----------------
This release of `ripple1d` incorporates preliminary support for hydraulic structures in HEC-RAS, improves the installation and setup process, and fixes several bugs.


Features Added
------------------

**Hydraulic Structures**

- All data associated with 1D structures that HEC-RAS supports is now included in the geometry extraction functions. (Endpoint exposing this will come in a future release). The extraction of data from the source models is now more robust and better handles different versions of RAS which wrote files slightly different.

- NWM reach models built from HEC-RAS source models that have the following structures will have structure data included:
   
  - Inline Structures
  - Bridges 
  - Culverts
  - Multiple Opening

.. note::
    Not included are lateral structures. Handling of lateral structures (wiers) will require additional assumptions/considerations to account for excess discharge (storage area, 2d area, another reach, etc).

**Conflation improvements**

- The conflation algorithm has been improved to accommodate models containing junctions. Where junctions exist, HEC-RAS rivers will be joined and the down stream XS (downstream of the junction) will be captured in the upstream model.
- Conflation now incorporates an additional downstream XS if available, extending beyond the NWM reach length to prevent gaps in FIM coverage.


**API**

- `ripple_version` is no longer a required argument for any endpoint.


Bug Fixes
----------
Numerous small bug fixes were made to enable the support of hydraulic structures. Other notable bugs include:

- HEC-RAS stations with length > 8 characters are now supported.
- Mangled profile names resulting from negative elevations producing FIM libraries has been fixed.