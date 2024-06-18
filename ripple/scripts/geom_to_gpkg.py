import json
import os
import shutil
import sys
import tempfile

import boto3
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pyproj import CRS

from ripple.errors import (
    DuplicateKeyEntriesError,
    NoCRSInferredError,
    NoKeyEntriesError,
    NotAPrjFile,
)
from ripple.ras import RasGeomText, RasManager


def read_inferred_crs_json(inferred_crs_json_path: str) -> pd.DataFrame:
    with open(inferred_crs_json_path, "r") as f:
        data = json.loads(f.read())

    return pd.DataFrame(data)


def update_key_base_path(df: pd.DataFrame, new_key_base: str, old_key_base: str) -> pd.DataFrame:
    df["key"] = df["key"].str.replace(old_key_base, new_key_base)
    return df


def geom_to_gpkg_local(ras_text_file_path: str, crs: CRS, output_gpkg_path: str):

    rm = RasManager(ras_text_file_path, crs=crs)
    rm.geom_flow_to_gpkg(output_gpkg_path)


def geom_to_gpkg_s3(ras_text_file_path: str, crs: CRS, output_gpkg_path: str, bucket: str):
    # load s3 credentials
    load_dotenv(find_dotenv())

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    client = session.client("s3")

    # get geom string
    response = client.get_object(Bucket=bucket, Key=ras_text_file_path)
    geom_string = response["Body"].read().decode("utf-8")

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read geom string and write geopackage
    geom = RasGeomText.from_str(geom_string, crs)
    geom.to_gpkg(output_gpkg_path)

    # move geopackage to s3
    client.upload_file(
        Bucket=bucket,
        Key=output_gpkg_path,
        Filename=temp_path,
    )
    shutil.rmtree(temp_dir)


def check_key(data: pd.DataFrame):

    if len(data) > 1:
        raise DuplicateKeyEntriesError(
            f"found duplicate 'key' entries in  {inferred_crs_json_path} for {ras_text_file_path}"
        )

    elif len(data) == 0:
        raise NoKeyEntriesError(
            f"Could not find any 'key' entries for {ras_text_file_path} in {inferred_crs_json_path}"
        )

    else:
        return True


def main(
    ras_text_file_path: str,
    inferred_crs_json_path: str,
    new_key_base: str = None,
    old_key_base: str = None,
    bucket: str = None,
):
    # create path name for gpkg
    if ras_text_file_path.endswith(".prj"):
        gpkg_path = ras_text_file_path.replace("prj", "gpkg")
    else:
        raise NotAPrjFile(f"{ras_text_file_path} does not have a '.prj' extension")

    # read inferred crs json and convert to dataframe
    crs_data = read_inferred_crs_json(inferred_crs_json_path)
    crs_data = update_key_base_path(crs_data, new_key_base, old_key_base)

    key_data = crs_data.loc[crs_data["key"] == ras_text_file_path]
    if check_key(crs_data):
        potential_crss = key_data["crscode2intersectratio"].iloc[0]  # dict

    # determine which crs to use if multiple matches. take the one with the greatest intersection ratio
    if potential_crss:
        crs = CRS([i for i in potential_crss if potential_crss[i] == max(potential_crss.values())][0])
    else:
        raise NoCRSInferredError(key_data["exc"])

    # read he geometry and write the geopackage
    if bucket:
        geom_to_gpkg_s3(ras_text_file_path, crs, gpkg_path, bucket)
    else:
        geom_to_gpkg_local(ras_text_file_path, crs, gpkg_path)


if __name__ == "__main__":

    ras_text_file_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\02-NT-00125\02-NT-00125_22bab4a4899d250f530f94066beccecd11383540\SENECA_RIVER_131\seneca.prj"
    inferred_crs_json_path = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models\ras_projects_crs_inference\ras_projects_crs_inference.json"
    new_key_base = r"C:\Users\mdeshotel\Downloads\12040101_Models\ripple\mip_models"
    old_key_base = r"T:\CCSI\TECH\owp\mip"
    bucket = None  # "fim"

    main(ras_text_file_path, inferred_crs_json_path, new_key_base, old_key_base, bucket)
