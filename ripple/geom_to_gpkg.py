import os
import shutil
import sys
import tempfile
from pyproj import CRS
import boto3
from dotenv import find_dotenv, load_dotenv

from .ras2 import RasGeomText

def main(ras_text_file_path: str, projection: str, gpkg_path: str):
    with open(projection, "r") as f:
        crs = f.read()
    ras_geom = RasGeomText(ras_text_file_path, projection=CRS(crs))
    ras_geom.to_gpkg(gpkg_path)

def main_s3(ras_text_file_path: str, projection: str, gpkg_path: str, bucket: str):
    # load s3 credentials
    load_dotenv(find_dotenv())

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    client = session.client("s3")

    # get geom string
    response = client.get_object(Bucket=bucket, Key=ras_text_file_path)
    geom_string = response["Body"].read().decode("utf-8")

    # get projection
    response = client.get_object(Bucket=bucket, Key=projection)
    projection = response["Body"].read().decode("utf-8")

    # make temp directory
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "temp.gpkg")

    # read geom string and write geopackage
    geom = RasGeomText.from_str(geom_string, projection)
    geom.to_gpkg(temp_path)

    # move geopackage to s3
    client.upload_file(
        Bucket=bucket,
        Key=gpkg_path,
        Filename=temp_path,
    )
    shutil.rmtree(temp_dir)


if __name__ == "__main__":

    ras_text_file_path = "stac/test-data/Baxter/Baxter.g02"
    projection = "stac/test-data/Baxter/CA_SPCS_III_NAVD88.prj"
    gpkg_path = ras_text_file_path.split(".")[0] + ".gpkg"
    bucket = "fim"

    main(ras_text_file_path, projection, gpkg_path, bucket)
    # main(*sys.argv[1:])
