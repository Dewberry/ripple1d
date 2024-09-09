Technical Reference
===================

Preliminary conflation metrics are calcuated to quantify alignment between the source RAS model and the source network reaches. Definitions and examples:

+------------------------+------------------------+-------------------+
| term                   | type                   | definition        |
+========================+========================+===================+
| reaches                | dict                   | Source network    |
|                        |                        | reaches that      |
|                        |                        | conflated with    |
|                        |                        | the RAS model.    |
|                        |                        | Reach ids are the |
|                        |                        | keys of the       |
|                        |                        | nested dicts      |
|                        |                        | which contain the |
|                        |                        | conflation        |
|                        |                        | results/metrics.  |
+------------------------+------------------------+-------------------+
| us_xs                  | dict                   | Contains relevant |
|                        |                        | HEC-RAS info for  |
|                        |                        | the most upstream |
|                        |                        | cross section     |
|                        |                        | associated with   |
|                        |                        | the reach.        |
+------------------------+------------------------+-------------------+
| ds_xs                  | dict                   | Contains relevant |
|                        |                        | HEC-RAS info for  |
|                        |                        | the most          |
|                        |                        | downstream cross  |
|                        |                        | section           |
|                        |                        | associated with   |
|                        |                        | the reach.        |
+------------------------+------------------------+-------------------+
| river                  | str                    | The river name    |
|                        |                        | for the cross     |
|                        |                        | section as it was |
|                        |                        | specified in the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| reach                  | str                    | The reach name    |
|                        |                        | for the cross     |
|                        |                        | section as it was |
|                        |                        | specified in the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| xs_id                  | float                  | The river station |
|                        |                        | for the cross     |
|                        |                        | section as it was |
|                        |                        | specified in the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| min_elevation          | float                  | The minimum       |
|                        |                        | elevation in the  |
|                        |                        | cross section     |
|                        |                        | station-elevation |
|                        |                        | data as it was    |
|                        |                        | specified in the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| max_elevation          | float                  | The maximum       |
|                        |                        | elevation in the  |
|                        |                        | cross section     |
|                        |                        | station-elevation |
|                        |                        | data as it was    |
|                        |                        | specified in the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| eclipsed               | bool                   | Specifies if the  |
|                        |                        | reach has been    |
|                        |                        | eclipsed          |
|                        |                        | (covered) by an   |
|                        |                        | upstream or       |
|                        |                        | downstream reach. |
+------------------------+------------------------+-------------------+
| low_flow               | int                    | The lower flow of |
|                        |                        | the source        |
|                        |                        | network data or   |
|                        |                        | the HEC-RAS data  |
|                        |                        | and reduced by    |
|                        |                        | 20%.              |
+------------------------+------------------------+-------------------+
| high_flow              | int                    | The higher flow   |
|                        |                        | of the source     |
|                        |                        | network data or   |
|                        |                        | the HEC-RAS data  |
|                        |                        | and increased by  |
|                        |                        | 20%.              |
+------------------------+------------------------+-------------------+
| network_to_id          | str                    | The to_id as      |
|                        |                        | specified in the  |
|                        |                        | attributes of the |
|                        |                        | input hydrofabric |
|                        |                        | layer.            |
+------------------------+------------------------+-------------------+
| metrics                | dict                   | Contains the      |
|                        |                        | conflation        |
|                        |                        | metrics for the   |
|                        |                        | reach.            |
+------------------------+------------------------+-------------------+
| xs                     | dict                   | Metrics           |
|                        |                        | describing the    |
|                        |                        | deviations        |
|                        |                        | between           |
|                        |                        | centerlines at    |
|                        |                        | cross section     |
|                        |                        | locationss for    |
|                        |                        | the source        |
|                        |                        | HEC-RAS model and |
|                        |                        | the hydrofabric.  |
+------------------------+------------------------+-------------------+
| centerline_offset      | dict                   | Metrics           |
|                        |                        | describing the    |
|                        |                        | distance between  |
|                        |                        | the point where   |
|                        |                        | the source        |
|                        |                        | HEC-RAS           |
|                        |                        | centerline        |
|                        |                        | intersects the    |
|                        |                        | cross sections    |
|                        |                        | and the point     |
|                        |                        | where the source  |
|                        |                        | network reach     |
|                        |                        | intersects the    |
|                        |                        | cross section.    |
+------------------------+------------------------+-------------------+
| thalweg_offset         | dict                   | Metrics           |
|                        |                        | describing the    |
|                        |                        | distance between  |
|                        |                        | the source        |
|                        |                        | HEC-RAS’s XS      |
|                        |                        | thalweg and the   |
|                        |                        | point where the   |
|                        |                        | source network    |
|                        |                        | reach intersects  |
|                        |                        | the cross         |
|                        |                        | section.          |
+------------------------+------------------------+-------------------+
| lengths                | dict                   | Metrics           |
|                        |                        | describing the    |
|                        |                        | distance between  |
|                        |                        | the source        |
|                        |                        | HEC-RAS’s XS      |
|                        |                        | thalweg and the   |
|                        |                        | point where the   |
|                        |                        | source network    |
|                        |                        | reach intersects  |
|                        |                        | the cross         |
|                        |                        | section.          |
+------------------------+------------------------+-------------------+
| ras                    | int                    | Length of the     |
|                        |                        | source HEC-RAS    |
|                        |                        | centerline        |
|                        |                        | between the most  |
|                        |                        | upstream and most |
|                        |                        | downstream cross  |
|                        |                        | sections          |
|                        |                        | associated with   |
|                        |                        | the source        |
|                        |                        | network.          |
+------------------------+------------------------+-------------------+
| network                | int                    | Length of source  |
|                        |                        | network between   |
|                        |                        | the most upstream |
|                        |                        | and most          |
|                        |                        | downstream cross  |
|                        |                        | sections          |
|                        |                        | associated with   |
|                        |                        | the source        |
|                        |                        | network reach.    |
+------------------------+------------------------+-------------------+
| coverage               | dict                   | Metrics           |
|                        |                        | describing the    |
|                        |                        | extent of         |
|                        |                        | coverage of the   |
|                        |                        | source network    |
|                        |                        | reach that is     |
|                        |                        | covered by the    |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| start                  | float                  | The upstream      |
|                        |                        | limit where the   |
|                        |                        | source HEC-RAS    |
|                        |                        | model provides    |
|                        |                        | coverage.         |
|                        |                        | Provided as a     |
|                        |                        | ratio of the      |
|                        |                        | entire source     |
|                        |                        | network reach     |
|                        |                        | length.           |
+------------------------+------------------------+-------------------+
| end                    | float                  | The downstream    |
|                        |                        | limit where the   |
|                        |                        | source HEC-RAS    |
|                        |                        | model provides    |
|                        |                        | coverage.         |
|                        |                        | Provided as a     |
|                        |                        | ratio of the      |
|                        |                        | entire source     |
|                        |                        | network reach.    |
+------------------------+------------------------+-------------------+
| overlapped_reaches     | list                   | Source network    |
|                        |                        | reaches that      |
|                        |                        | intersect the     |
|                        |                        | downstream most   |
|                        |                        | cross sections of |
|                        |                        | the current       |
|                        |                        | reach.            |
+------------------------+------------------------+-------------------+
| eclipsed_reaches       | list                   | Reaches that are  |
|                        |                        | eclipsed          |
|                        |                        | (covered) by the  |
|                        |                        | cross sections of |
|                        |                        | the current       |
|                        |                        | reach.            |
+------------------------+------------------------+-------------------+
| metadata               | dict                   | Metadata for the  |
|                        |                        | conflation.       |
+------------------------+------------------------+-------------------+
| source_network         | dict                   | Data summarizing  |
|                        |                        | the source        |
|                        |                        | network.          |
+------------------------+------------------------+-------------------+
| file_name              | str                    | Source network    |
|                        |                        | file name.        |
+------------------------+------------------------+-------------------+
| version                | str                    | Source network    |
|                        |                        | version.          |
+------------------------+------------------------+-------------------+
| type                   | str                    | Source network    |
|                        |                        | type.             |
+------------------------+------------------------+-------------------+
| conflation_png         | str                    | A png depicting   |
|                        |                        | the conflation    |
|                        |                        | results.          |
+------------------------+------------------------+-------------------+
| confl                  | str                    | The version of    |
| ation_ripple1d_version |                        | ripple1d used to  |
|                        |                        | conflate the      |
|                        |                        | source HEC-RAS    |
|                        |                        | model with the    |
|                        |                        | source network.   |
+------------------------+------------------------+-------------------+
| me                     | str                    | The version of    |
| trics_ripple1d_version |                        | ripple1d used to  |
|                        |                        | compute the       |
|                        |                        | conflation        |
|                        |                        | metric.           |
+------------------------+------------------------+-------------------+
| source_ras_model       | dict                   | Metadata for the  |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| stac_api               | str                    | The stac api url  |
|                        |                        | containing the    |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| stac_collection_id     | str                    | The stac          |
|                        |                        | collection        |
|                        |                        | containing the    |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| stac_item_id           | str                    | The stac item for |
|                        |                        | the source        |
|                        |                        | HEC-RAS model.    |
+------------------------+------------------------+-------------------+
| source_ras_files       | dict                   | files for the     |
|                        |                        | souce HEC-RAS     |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| geometry               | str                    | The geometry file |
|                        |                        | used from the     |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| forcing                | str                    | The forcing file  |
|                        |                        | used from the     |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| project-file           | str                    | The project file  |
|                        |                        | used from the     |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| plan                   | str                    | The plan file     |
|                        |                        | used from the     |
|                        |                        | source HEC-RAS    |
|                        |                        | model.            |
+------------------------+------------------------+-------------------+
| length_units           | str                    | Units for the     |
|                        |                        | lengths specified |
|                        |                        | in the conflation |
|                        |                        | json.             |
+------------------------+------------------------+-------------------+
| flow_units             | str                    | Units for the     |
|                        |                        | flows specified   |
|                        |                        | in the conflation |
|                        |                        | json.             |
+------------------------+------------------------+-------------------+

Example output containing conflation metrics: 

.. code:: json

   {
       "reaches": {
           "11908582": {
               "us_xs": {
                   "river": "Patuxent River",
                   "reach": "1",
                   "min_elevation": -10.6,
                   "max_elevation": 93.12,
                   "xs_id": 32805.59
               },
               "ds_xs": {
                   "river": "Patuxent River",
                   "reach": "1",
                   "min_elevation": -8.5,
                   "max_elevation": 53.21,
                   "xs_id": 26469.46
               },
               "eclipsed": false,
               "low_flow_cfs": 2025,
               "high_flow_cfs": 19969,
               "network_to_id": "11908588",
               "metrics": {
                   "xs": {
                       "centerline_offset": {
                           "count": 10,
                           "mean": 13,
                           "std": 8,
                           "min": 1,
                           "10%": 2,
                           "20%": 4,
                           "30%": 9,
                           "40%": 12,
                           "50%": 14,
                           "60%": 16,
                           "70%": 20,
                           "80%": 22,
                           "90%": 23,
                           "100%": 24,
                           "max": 24
                       },
                       "thalweg_offset": {
                           "count": 10,
                           "mean": 40,
                           "std": 34,
                           "min": 1,
                           "10%": 2,
                           "20%": 8,
                           "30%": 16,
                           "40%": 28,
                           "50%": 39,
                           "60%": 45,
                           "70%": 49,
                           "80%": 64,
                           "90%": 92,
                           "100%": 98,
                           "max": 98
                       }
                   },
                   "lengths": {
                       "ras": 6377,
                       "network": 6334,
                       "network_to_ras_ratio": 0.99
                   },
                   "coverage": {
                       "start": 0.09,
                       "end": 1
                   }
               },
               "overlapped_reaches": [
                   {
                       "id": "11908588",
                       "overlap": 757
                   }
               ],
               "eclipsed_reaches": [
                   "11908584",
                   "11908586"
               ]
           },
       },
       "metadata": {
           "source_network": {
               "file_name": "flows.parquet",
               "version": "2.1",
               "type": "nwm_hydrofabric"
           },
           "conflation_png": "PatuxentRiver.conflation.png",
           "conflation_ripple1d_version": "0.4.2",
           "metrics_ripple1d_version": "0.4.2",
           "source_ras_model": {
               "stac_api": "https://stac2.dewberryanalytics.com",
               "stac_collection_id": "ebfe-12090301_LowerColoradoCummins",
               "stac_item_id": "137a9667-e5cf-4cea-b6ec-2e882a42fdc8",
               "source_ras_files": {
                   "geometry": "PatuxentRiver.g01",
                   "forcing": "PatuxentRiver.f01",
                   "project-file": "PatuxentRiver.prj",
                   "plan": "PatuxentRiver.p06"
               }
           },
           "length_units": "feet",
           "flow_units": "cfs"
       }
   }
