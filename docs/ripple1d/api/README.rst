Overview
--------

Ripple ships with a REST API server leverages huey for concurrent task
(job) execution, and Flask as the user HTTP REST interface into the huey
system. The HTTP endpoints adhere closely to `OGC Process
API <https://ogcapi.ogc.org/processes/overview.html>`__ standards.

This document provides an overview of the REST endpoints available in
the application. The API allows you to manage various tasks related to
the creation and execution of models, terrain, and other processes.


Endpoints
---------

Health Check
~~~~~~~~~~~~

-  **Endpoint**: ``/ping``
-  **Method**: ``GET``
-  **Description**: Check the health of the service.
-  **Response**:

   -  ``200 OK`` with JSON ``{"status": "healthy"}``

Task Execution Endpoints
~~~~~~~~~~~~~~~~~~~~~~~~

-  **Endpoint**: ``/processes/extract_submodel/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to create a new GeoPackage.

-  **Endpoint**: ``/processes/create_ras_terrain/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to create a new RAS terrain.

-  **Endpoint**: ``/processes/create_model_run_normal_depth/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to calculate the initial normal
      depth.

-  **Endpoint**: ``/processes/run_incremental_normal_depth/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to calculate the incremental
      normal depth.

-  **Endpoint**: ``/processes/run_known_wse/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to calculate the water surface
      elevation (WSE) based on known inputs.

-  **Endpoint**: ``/processes/create_fim_lib/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to create a FIM library.

-  **Endpoint**: ``/processes/nwm_reach_model_stac/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to create a stac item from a FIM
      model.

-  **Endpoint**: ``/processes/fim_lib_stac/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task to create a stac item from a FIM
      library.

-  **Endpoint**: ``/processes/test/execution``

   -  **Method**: ``POST``
   -  **Description**: Test the execution and monitoring of an
      asynchronous task.

-  **Endpoint**: ``/processes/sleep/execution``

   -  **Method**: ``POST``
   -  **Description**: Enqueue a task that sleeps for 15 seconds.

Job Management Endpoints
~~~~~~~~~~~~~~~~~~~~~~~~

-  **Endpoint**: ``/jobs/<task_id>``

   -  **Method**: ``GET``
   -  **Description**: Retrieve OGC status and result for one job.
   -  **Query Parameters**:

      -  ``tb``: Choices are ``['true', 'false']``. Defaults to
         ``false``. If ``true``, the job result’s traceback will be
         included in the response.

-  **Endpoint**: ``/jobs``

   -  **Method**: ``GET``
   -  **Description**: Retrieve OGC status and result for all jobs.
   -  **Query Parameters**:

      -  ``tb``: Choices are ``['true', 'false']``. Defaults to
         ``false``. If ``true``, each job result’s traceback will be
         included in the response.

-  **Endpoint**: ``/jobs/<task_id>``

   -  **Method**: ``DELETE``
   -  **Description**: Dismiss a specific task by its ID.


Submodules
---------