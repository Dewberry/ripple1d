
Ripple
======

Ripple contains three principal modules: Conflate, Ops, and Utils. Each module contains a number of submodules 
that provide the functionality needed to repurpose HEC-RAS models in the development of FIMs. The Conflate module is 
responsible for mapping the HEC-RAS model reaches to reaches in the National Water Model hydrofabric. The Ops module 
is responsible for managing the operation of Extract, Transform, and Load (ETL) processes, including creating, running, and 
mapping results from NWM reach-HEC-RAS models. The Utils module contains helper functions and utilities.

.. toctree::
   :maxdepth: 2
   :caption: Modules:

   conflate/index
   ops/index
   utils/index

.. toctree::
   :maxdepth: 2
   :caption: Submodules:

   data_model
   errors
   ras_to_gpkg
   ras
   ripple_logger