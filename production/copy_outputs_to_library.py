"""
Copy all tifs from the model output folders to the FIM library location while preserving folder structure
"""

import os
import shutil

root_dir = r"D:\Users\abdul.siddiqui\workbench\projects\trial_run_ripple"
library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\trial_run_ripple\library"


if not os.path.exists(library_dir):
    os.makedirs(library_dir)

for subdir, dirs, files in os.walk(root_dir):
    if "output" in dirs:
        output_dir = os.path.join(subdir, "output")
        for sub_output_dir, subdirs, output_files in os.walk(output_dir):
            for file in output_files:
                if file.endswith(".tif"):
                    file_path = os.path.join(sub_output_dir, file)

                    relative_path = os.path.relpath(sub_output_dir, output_dir)
                    target_dir = os.path.join(library_dir, relative_path)
                    if not os.path.exists(target_dir):
                        os.makedirs(target_dir)
                    shutil.copy(file_path, target_dir)

print("All .tif files have been copied to the library directory preserving the folder structure.")
