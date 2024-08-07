"""Build wheel ensuring Windows OS and python >=3.10."""

import os
import subprocess
import sys

if os.name != "nt":
    sys.exit("Windows OS is required to build this package")

if sys.version_info < (3, 10):
    sys.exit("Python 3.10 or greater is required to build this package.")

platform_tag = "win_amd64"

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
