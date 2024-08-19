Change Log for Users
=====================

Go to the `Releases <https://github.com/Dewberry/ripple1d/releases.html>`_  page for a list of all releases.


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