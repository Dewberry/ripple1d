"""General utils used by ripple1d."""

from __future__ import annotations

import glob
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from shapely.geometry import Point, Polygon

from ripple1d.errors import (
    RASComputeError,
    RASComputeMeshError,
    RASGeometryError,
    RASStoreAllMapsError,
)
from ripple1d.utils.s3_utils import list_keys

load_dotenv(find_dotenv())


def decode(df: pd.DataFrame):
    """Decode all string columns in a pandas DataFrame."""
    for c in df.columns:
        df[c] = df[c].str.decode("utf-8")
    return df


def get_path(expected_path: str, client: boto3.client = None, bucket: str = None) -> str:
    """Get the path for a file."""
    if client and bucket:
        path = Path(expected_path)
        prefix = path.parent.as_posix().replace("s3:/", "s3://")
        paths = list_keys(client, bucket, prefix, path.suffix)
    else:
        prefix = os.path.dirname(expected_path)
        paths = glob.glob(rf"{prefix}\*{os.path.splitext(expected_path)[1]}")

    if expected_path in paths:
        return expected_path
    else:
        for path in paths:
            if path.endswith(Path(expected_path).suffix):
                return path


def xs_concave_hull(xs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Compute and return the concave hull (polygon) for a set of cross sections (lines all facing the same direction)."""
    points = xs.boundary.explode(index_parts=True).unstack()
    points_last_xs = [Point(coord) for coord in xs["geometry"].iloc[-1].coords]
    points_first_xs = [Point(coord) for coord in xs["geometry"].iloc[0].coords[::-1]]

    polygon = Polygon(points_first_xs + list(points[0]) + points_last_xs + list(points[1])[::-1])

    return gpd.GeoDataFrame({"geometry": [polygon]}, geometry="geometry", crs=xs.crs)


def search_contents(lines: list, search_string: str, token: str = "=", expect_one: bool = True) -> list[str]:
    """Split a line by a token and returns the second half of the line if the search_string is found in the first half."""
    results = []
    for line in lines:
        if f"{search_string}{token}" in line:
            results.append(line.split(token)[1])

    if expect_one and len(results) > 1:
        raise ValueError(f"expected 1 result, got {len(results)}")
    elif expect_one and len(results) == 0:
        raise ValueError("expected 1 result, no results found")
    elif expect_one and len(results) == 1:
        return results[0]
    else:
        return results


def replace_line_in_contents(lines: list, search_string: str, replacement: str, token: str = "="):
    """Split a line by a token and replaces the second half of the line (for the first occurence only!)."""
    updated = 0
    for i, line in enumerate(lines):
        if f"{search_string}{token}" in line:
            updated += 1
            lines[i] = line.split(token)[0] + token + replacement
    if updated == 0:
        raise ValueError(f"search_string: {search_string} not found in lines")
    elif updated > 1:
        raise ValueError(f"expected 1 result, got {updated} occurences of {replacement}")
    else:
        return lines


def text_block_from_start_end_str(start_str: str, end_str: str, lines: list, additional_lines: int = None) -> list[str]:
    """Search for an exact match to the start_str and return all lines from there to a line that contains the end_str."""
    start_str = handle_spaces(start_str, lines)
    results = []
    in_block = False
    for i, line in enumerate(lines):
        if line == start_str:
            in_block = True
            results.append(line)
            continue

        if in_block:
            if end_str in line:
                if additional_lines:
                    for additional_line in lines[i : i + additional_lines]:
                        results.append(additional_line)
                    return results
                else:
                    return results
            else:
                results.append(line)
    return results


def text_block_from_start_str_to_empty_line(start_str: str, lines: list) -> list[str]:
    """Search for an exact match to the start_str and return all lines from there to the next empty line."""
    start_str = handle_spaces(start_str, lines)
    results = []
    in_block = False
    for line in lines:
        if line == start_str:
            in_block = True
            results.append(line)
            continue

        if in_block:
            if line == "":
                results.append(line)
                return results
            else:
                results.append(line)
    return results


def text_block_from_start_str_length(start_str: str, number_of_lines: int, lines: list) -> list[str]:
    """Search for an exact match to the start token and return a number of lines equal to number_of_lines."""
    start_str = handle_spaces(start_str, lines)
    results = []
    in_block = False
    for line in lines:
        if line == start_str:
            in_block = True
            continue
        if in_block:
            if len(results) >= number_of_lines:
                return results
            else:
                results.append(line)


def data_pairs_from_text_block(lines: list[str], width: int) -> list[tuple[float]]:
    """Split lines at given width to get paired data string. Split the string in half and convert to tuple of floats."""
    pairs = []
    for line in lines:
        for i in range(0, len(line), width):
            x = line[i : int(i + width / 2)]
            y = line[int(i + width / 2) : int(i + width)]
            pairs.append((float(x), float(y)))

    return pairs


def handle_spaces(line: str, lines: list[str]):
    """Handle spaces in the line."""
    if line in lines:
        return line
    elif handle_spaces_arround_equals(line.rstrip(" "), lines):
        return handle_spaces_arround_equals(line.rstrip(" "), lines)
    elif handle_spaces_arround_equals(line + " ", lines) in lines:
        return handle_spaces_arround_equals(line + " ", lines)
    else:
        raise ValueError(f"line: {line} not found in lines")


def handle_spaces_arround_equals(line: str, lines: list[str]) -> str:
    """Handle spaces in the line."""
    if line in lines:
        return line
    elif "= " in line:
        if line.replace("= ", "=") in lines:
            return line.replace("= ", "=")
    else:
        return line.replace("=", "= ")


def assert_no_mesh_error(compute_message_file: str, require_exists: bool):
    """Scan *.computeMsgs.txt for errors encountered. Raise RASComputeMeshError if found."""
    try:
        with open(compute_message_file) as f:
            content = f.read()
    except FileNotFoundError:
        if require_exists:
            raise
    else:
        for line in content.splitlines():
            if "error generating mesh" in line.lower():
                raise RASComputeMeshError(
                    f"'error generating mesh' found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
                )


def assert_no_ras_geometry_error(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered."""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "geometry writer failed" in line.lower() or "error processing geometry" in line.lower():
            raise RASGeometryError(
                f"geometry error found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )


def assert_no_ras_compute_error_message(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered."""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "ERROR:" in line:
            raise RASComputeError(
                f"'ERROR:' found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )


def assert_no_store_all_maps_error_message(compute_message_file: str):
    """Scan *.computeMsgs.txt for errors encountered."""
    with open(compute_message_file) as f:
        content = f.read()
    for line in content.splitlines():
        if "error executing: storeallmaps" in line.lower():
            raise RASStoreAllMapsError(
                f"{repr(line)} found in {compute_message_file}. Full file content:\n{content}\n^^^ERROR^^^"
            )
