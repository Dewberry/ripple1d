"""
Ripple1D Package.

This package provides tools and utilities for managing 1-dimensional HEC-RAS simulations.
"""

import os

import toml


def get_version():
    """Get version info for build."""
    pyproject_path = os.path.join(os.path.dirname(__file__), "__version__.py")
    with open(pyproject_path, "r") as f:
        return f.readline().strip()


try:
    __version__ = get_version()
except FileNotFoundError as e:
    __version__ == ""
