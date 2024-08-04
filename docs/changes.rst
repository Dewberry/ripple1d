Change Log
==========


Go to the `Releases <https://github.com/Dewberry/ripple/releases.html>`__  page for a list of all releases.

0.1.0 (2024-7-3)
~~~~~~~~~~~~~~~

**Beta Release**

* Initial classes, methods and functions for:

  - Extracting geometry from HEC-RAS 1D .g** files

  - Creating NWM reach-based HEC-RAS models by subsetting existing 1D models

  - Creating and managing normal depth and Known Water Surface Elevation (kwse) simulations

  - Creating HEC-RAS terrain files

  - Exporting simulation results to depth grids (cloud optimized geotiffs)
  
* API for managing parallel execution with Huey queue for task scheduling

* Example scripts for using the library natively (without the API)