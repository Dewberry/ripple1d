"""
Copy all fims from the submodel folders to the FIM library location
"""

import os
import shutil

submodels_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"

for submodel in os.listdir(submodels_dir):
    dirs = os.listdir(f"{submodels_dir}/{submodel}")
    if "fims" in dirs:
        source_path = os.path.join(submodels_dir, submodel, "fims")
        destination_path = os.path.join(library_dir, submodel)
        print(source_path, destination_path)
        shutil.copytree(source_path, destination_path)

print("All fims have been copied to the library directory.")
