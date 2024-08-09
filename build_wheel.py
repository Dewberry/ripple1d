"""Build wheel ensuring Windows OS and python >=3.10."""

import os
import subprocess
import sys

import toml


def get_version():
    """Get version info for build."""
    pyproject_path = os.path.join(os.path.dirname(__file__), "pyproject.toml")
    with open(pyproject_path, "r") as f:
        pyproject_data = toml.load(f)
    return pyproject_data["project"]["version"]


if os.name != "nt":
    sys.exit("Windows OS is required to build this package")

if sys.version_info < (3, 10):
    sys.exit("Python 3.10 or greater is required to build this package.")

platform_tag = "win_amd64"


with open(os.path.join(os.path.dirname(__file__), "ripple1d", "__version__.py"), "w") as f:
    f.write('"""Auto-generated ripple1d version file."""\n')
    f.write(f'__version__ = "{get_version()}"')

subprocess.check_call([os.sys.executable, "-m", "build"])


dist_dir = "dist"
for filename in os.listdir(dist_dir):
    if filename.endswith(".whl"):
        parts = filename.split("-")
        parts[2] = "py310"
        parts[-1] = platform_tag + ".whl"
        new_filename = "-".join(parts)
        os.rename(os.path.join(dist_dir, filename), os.path.join(dist_dir, new_filename))
        break
