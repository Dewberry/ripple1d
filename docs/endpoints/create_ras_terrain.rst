create_ras_terrain
##################

**URL:** ``/processes/create_ras_terrain/execution``

**Method:** ``POST``

**Description:**

.. autofunction:: ripple1d.ops.ras_terrain.create_ras_terrain
    :no-index:

**Terrain Agreement:**

Terrain agreement metrics are written to a json file with suffix ".terrain_agreement.json" in the model directory.  The json will have the following schema:

.. code-block::
        {
        "type": "object",
        "properties": {
            "xs": {
            "type": "object",
            "additional_properties": {
                "type": "object",
                "properties": {
                "elevation": {
                    "type": "object",
                    "properties": {
                    "inundation_overlap": { "type": "number" },
                    "flow_area_overlap": { "type": "number" },
                    "top_width_agreement": { "type": "number" },
                    "flow_area_agreement": { "type": "number" },
                    "hydraulic_radius_agreement": { "type": "number" },
                    "residuals": {
                        "type": "object",
                        "properties": {
                        "mean": { "type": "number" },
                        "std": { "type": "number" },
                        "max": { "type": "number" },
                        "min": { "type": "number" },
                        "p_25": { "type": "number" },
                        "p_50": { "type": "number" },
                        "p_75": { "type": "number" },
                        "rmse": { "type": "number" },
                        "normalized_rmse": { "type": "number" }
                        }
                    }
                    }
                },
                "summary": {
                    "type": "object",
                    "properties": {
                    "inundation_overlap": { "type": "number" },
                    "flow_area_overlap": { "type": "number" },
                    "top_width_agreement": { "type": "number" },
                    "flow_area_agreement": { "type": "number" },
                    "hydraulic_radius_agreement": { "type": "number" },
                    "residuals": {
                        "type": "object",
                        "properties": {
                        "mean": { "type": "number" },
                        "std": { "type": "number" },
                        "max": { "type": "number" },
                        "min": { "type": "number" },
                        "p_25": { "type": "number" },
                        "p_50": { "type": "number" },
                        "p_75": { "type": "number" },
                        "rmse": { "type": "number" },
                        "normalized_rmse": { "type": "number" }
                        }
                    },
                    "r_squared": { "type": "number" },
                    "spectral_angle": { "type": "number" },
                    "spectral_correlation": { "type": "number" },
                    "correlation": { "type": "number" },
                    "max_cross_correlation": { "type": "number" },
                    "thalweg_elevation_difference": { "type": "number" }
                    }
                }
                }
            }
            },
            "summary": {
            "type": "object",
            "properties": {
                "inundation_overlap": { "type": "number" },
                "flow_area_overlap": { "type": "number" },
                "top_width_agreement": { "type": "number" },
                "flow_area_agreement": { "type": "number" },
                "hydraulic_radius_agreement": { "type": "number" },
                "r_squared": { "type": "number" },
                "spectral_angle": { "type": "number" },
                "spectral_correlation": { "type": "number" },
                "correlation": { "type": "number" },
                "max_cross_correlation": { "type": "number" },
                "thalweg_elevation_difference": { "type": "number" }
            }
            }
        }
        }
