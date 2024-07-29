import os
import subprocess

output_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\scenarios"
output_subfolder = "wfsj_12040101"

library_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"
library_db_path = r"D:/Users/abdul.siddiqui/workbench/projects/production/library.sqlite"
flow_files_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\scenarios\flow_files"
start_csv = r"D:\Users\abdul.siddiqui\workbench\projects\wfsj_huc8\startReaches.csv"
fim_format = "tif"

if not os.path.exists(os.path.join(output_dir, output_subfolder)):
    os.mkdir(os.path.join(output_dir, output_subfolder))


for flow_file in os.listdir(flow_files_dir):
    if flow_file.endswith(".csv"):
        flow_file_path = os.path.join(flow_files_dir, flow_file)
        basename = os.path.splitext(flow_file)[0]
        control_csv = os.path.join(output_dir, output_subfolder, f"{basename.replace('flows', 'controls')}.csv")
        fim_output = os.path.join(output_dir, output_subfolder, f"{basename.replace('flows', 'fim')}.{fim_format}")

        cmd_controls = [
            "flows2fim.exe",
            "controls",
            "-db",
            library_db_path,
            "-f",
            flow_file_path,
            "-c",
            control_csv,
            "-scsv",
            start_csv,
        ]
        subprocess.run(cmd_controls, shell=True, check=True)

        cmd_fim = [
            "flows2fim.exe",
            "fim",
            "-lib",
            library_path,
            "-c",
            control_csv,
            "-o",
            fim_output,
            "-fmt",
            fim_format,
        ]
        subprocess.run(cmd_fim, shell=True, check=True)

print("All flow files have been processed.")
