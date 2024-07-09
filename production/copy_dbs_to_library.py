"""Copy all .db from the model output folders to the dbs directory location while preserving folder structure"""

import os
import shutil

root_dir = r"D:\Users\abdul.siddiqui\workbench\projects\trial_run_ripple"
library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\trial_run_ripple\dbs"

if not os.path.exists(library_dir):
    os.makedirs(library_dir)

for subdir, dirs, files in os.walk(root_dir):
    if "output" in dirs:
        output_dir = os.path.join(subdir, "output")
        for sub_output_dir, subdirs, output_files in os.walk(output_dir):
            for file in output_files:
                if file.endswith(".db"):
                    file_path = os.path.join(sub_output_dir, file)
                    shutil.copy(file_path, library_dir)

print("All .db files have been copied to the dbs directory.")
