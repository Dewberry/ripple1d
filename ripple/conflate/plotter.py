import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import geopandas as gpd
from io import BytesIO
from pathlib import Path
from boto3 import Session

from .ras1d import RasFimConflater


def plot_conflation_results(
    rfc: RasFimConflater,
    fim_stream: gpd.GeoDataFrame,
    key: str,
    bucket: str = None,
    s3_client: Session.client = None,
):
    _, ax = plt.subplots(figsize=(10, 10))

    # Plot the centerline and cross-sections first
    rfc.ras_centerline.plot(
        ax=ax, color="black", label="RAS Centerline", alpha=0.5, linestyle="dashed"
    )
    rfc.ras_xs.plot(ax=ax, color="green", label="RAS XS", markersize=2, alpha=0.2)

    # Create a colormap that maps each branch_id to a color
    unique_branch_ids = fim_stream["branch_id"].unique()
    colors = plt.cm.viridis(np.linspace(0, 1, len(unique_branch_ids)))
    colormap = dict(zip(unique_branch_ids, colors))

    # Plot the fim_stream using the colormap
    fim_stream["color"] = fim_stream["branch_id"].map(colormap)
    fim_stream.plot(color=fim_stream["color"], ax=ax, linewidth=2, alpha=0.8)

    # Get the current axis limits
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()

    # Create a mask for the branches that fall within the axis limits
    mask = rfc.nwm_branches.geometry.apply(
        lambda geom: xlim[0] <= geom.bounds[0] <= xlim[1]
        and ylim[0] <= geom.bounds[1] <= ylim[1]
    )

    # Plot the branches that fall within the axis limits
    rfc.nwm_branches[mask].plot(ax=ax, color="blue", linewidth=1, alpha=0.3)

    # Create a custom legend using the colormap
    patches = [
        mpatches.Patch(color=colormap[branch_id], label=f"Branch {branch_id}")
        for branch_id in unique_branch_ids
    ]

    # Add a patch for the ras_centerline
    patches.append(
        mpatches.Patch(color="black", label="RAS Centerline", linestyle="dashed")
    )
    patches.append(mpatches.Patch(color="blue", label="Nearby NWM Branches", alpha=0.3))
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
        with open(Path(key).name, "w") as f:
            plt.savefig(f, format="png")

    plt.close()
