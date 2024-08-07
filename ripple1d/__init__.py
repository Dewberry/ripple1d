"""
Ripple1D Package.

This package provides tools and utilities for managing 1-dimensional HEC-RAS simulations.
"""

import os

import toml


def get_version():
    """Get version info for build."""
    pyproject_path = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
    with open(pyproject_path, "r") as f:
        pyproject_data = toml.load(f)
    return pyproject_data["project"]["version"]


__version__ = get_version()
