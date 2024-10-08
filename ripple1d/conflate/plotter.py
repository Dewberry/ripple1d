"""Plotting functions for geometry data."""

import logging
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import shapely
from boto3 import Session

from .rasfim import RasFimConflater


def plot_conflation_results(
    rfc: RasFimConflater,
    fim_stream: gpd.GeoDataFrame,
    key: str,
    bucket: str = None,
    s3_client: Session.client = None,
    limit_plot_to_nearby_reaches: bool = True,
):
    """Create/write png to s3. The png contains RAS centerline and cross sections and nearby NWM branches."""
    _, ax = plt.subplots(figsize=(10, 10))

    # Plot the centerline and cross-sections first
    rfc.ras_centerlines.plot(ax=ax, color="black", label="RAS Centerline", alpha=0.5, linestyle="dashed")
    rfc.ras_xs.plot(ax=ax, color="green", label="RAS XS", markersize=2, alpha=0.2)
    if rfc.ras_structures is not None:
        rfc.ras_structures.plot(ax=ax, color="grey", label="RAS Structure", markersize=2, alpha=0.2)

    # Get the current axis limits and create a rectangle geometry
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    bounds = shapely.geometry.box(xlim[0], ylim[0], xlim[1], ylim[1])

    zoom_factor = 1  # Adjust this value to change the zoom level

    # Calculate the range of x and y
    x_range = bounds.bounds[2] - bounds.bounds[0]
    y_range = bounds.bounds[3] - bounds.bounds[1]

    min_x = bounds.bounds[0] - x_range * (zoom_factor - 1) / 2
    max_x = bounds.bounds[2] + x_range * (zoom_factor - 1) / 2
    min_y = bounds.bounds[1] - y_range * (zoom_factor - 1) / 2
    max_y = bounds.bounds[3] + y_range * (zoom_factor - 1) / 2

    adjusted_bounds = shapely.geometry.box(min_x, min_y, max_x, max_y)
    # Add a patch for the ras_centerline
    patches = [mpatches.Patch(color="black", label="RAS Centerline", linestyle="dashed")]

    # Add a patch for nearby reaches
    patches.append(mpatches.Patch(color="blue", label="Nearby NWM reaches", alpha=0.3))

    # Plot the reaches that fall within the axis limits
    rfc.local_nwm_reaches().plot(ax=ax, color="blue", linewidth=1, alpha=0.3)

    if limit_plot_to_nearby_reaches:
        # Create a colormap that maps each reach_id to a color
        unique_reach_ids = fim_stream["ID"].unique()
        colors = plt.cm.viridis(np.linspace(0, 1, len(unique_reach_ids)))
        colormap = dict(zip(unique_reach_ids, colors))

        # Plot the fim_stream using the colormap
        fim_stream["color"] = fim_stream["ID"].map(colormap)
        fim_stream.plot(color=fim_stream["color"], ax=ax, linewidth=2, alpha=0.8)
        # Create a custom legend using the colormap
        patches.extend(
            [mpatches.Patch(color=colormap[reach_id], label=f"reach {reach_id}") for reach_id in unique_reach_ids]
        )

    zoom_factor = 3  # Adjust this value to change the zoom level

    # Calculate the range of x and y
    x_range = bounds.bounds[2] - bounds.bounds[0]
    y_range = bounds.bounds[3] - bounds.bounds[1]

    # Set the axis limits to the bounds, expanded by the zoom factor
    ax.set_xlim(
        min_x,
        max_x,
    )
    ax.set_ylim(
        min_y,
        max_y,
    )

    ax.legend(handles=patches, handleheight=0.005)
    ax.set_xticks([])
    ax.set_yticks([])

    plt.tight_layout()

    if s3_client:
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=buf, ContentType="image/png")
    else:
        output_png = str(Path(rfc.source_model_directory) / Path(key).name)
        plt.savefig(output_png, format="png")

    plt.close()
