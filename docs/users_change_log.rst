Change Log for Users
=====================

Go to the `Releases <https://github.com/Dewberry/ripple1d/releases.html>`_  page for a list of all releases.

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