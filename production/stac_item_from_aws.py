import json
import time
import os
import shutil
import hashlib
import re

import logging
from datetime import datetime
import tempfile

import glob

import ripple1d.__version__ as version
from ripple1d.ras_to_gpkg import gpkg_from_ras, geom_flow_to_gpkg, RasProject
from ripple1d.ops.stac_item import rasmodel_to_stac,make_stac_assets
from ripple1d.data_model import RasModelStructure, RippleSourceModel
from ripple1d.utils.s3_utils import get_basic_object_metadata
from ripple1d.utils.ripple_utils import prj_is_ras
from ripple1d.api.log import initialize_log


def download_model(s3_client,bucket, assets, tmp_dir):
    """ Download all ras files in the directory of .prj file."""
    logging.debug(f'Downloading assets: {assets}')
    RELEVANT_FILE_FINDER = re.compile(r'\.[Pp][Rr][Jj]|\.[Gg]\d{2}|\.[Pp]\d{2}|\.[FfUuQq]\d{2}')
    download_keys = [s for s in assets.keys() if RELEVANT_FILE_FINDER.fullmatch(pathlib.Path(s).suffix)]
    for key in download_keys:
        s3_client.download_file(bucket, key, os.path.join(tmp_dir, Path(key).name))


def get_assets(s3_resource, bucket, keys)-> dict:
    logging.info(f'Finding assets associated with prefix {prefix}')
    asset_dict = {}
    for key in keys:
        obj = s3_resource.Bucket(bucket).Object(key)
        asset_dict[key] = {
            'href': f's3://{bucket}/{key}',
            'meta': get_basic_object_metadata(obj)
            }
    return asset_dict


def process_key(bucket:str, key:str, crs:str):
    """Convert RAS model associated with a .prj S3 key to stac"""
    logging.info(f'Processing key: {key}')

    s3_access = {}
    _, s3_client, s3_resource = init_s3_resources()

    # Get assets and Download model
    prefix = '/'.join(key.split('/')[:-1]) + '/'
    keys = list_keys(s3_client, bucket=bucket, prefix=prefix)

    assets = get_assets(s3_access, keys)

    with tempfile.TemporaryDirectory() as tmp_dir:
        logging.debug(f'Temp folder created at {tmp_dir}')

        download_model(s3_access, assets, tmp_dir)

        # Find ras .prj file
        prjs = glob.glob(f"{tmp_dir}/*.prj")
        prjs = [prj for prj in prjs if prj_is_ras(prj)]
        if len(prjs) != 1:
            raise KeyError(f"Expected 1 RAS file, found {len(prjs)}: {prjs}")

        # Make a geopackage
        logging.info(f'Making geopackage for {key}')
        rp = RasProject(ras_prj_path)
        ras_gpkg_path = ras_prj_path.replace(".prj", ".gpkg")
        geom_flow_to_gpkg(rp, crs, ras_gpkg_path, {})

        # Create a STAC item
        logging.info(f'Making stac item for {key}')
        rm = RippleSourceModel(ras_prj_path, crs)
        stac = rasmodel_to_stac(rm)

        # Upload png and gpkg to s3
        # TODO: make function
        out_png_key = f'ebfedata-derived/stac/v{ripple1d.__version__}-rc/{prefix.replace('ebfedata/', '')}{os.path.basename(rm.thumbnail_png)}'
        s3_client.upload_file(Bucket=bucket, Key=out_png_key, Filename=rm.thumbnail_png)
        obj = s3_resource.Bucket(bucket).Object(out_png_key)
        meta = get_basic_object_metadata(obj)
        assets['Thumbnail'] = {
            'href': f'{bucket}.amazonaws.com/{out_png_key}',
            'meta': meta
            }

        out_gpkg_key = f'ebfedata-derived/gpkgs/v{ripple1d.__version__}-rc/{prefix.replace('ebfedata/', '')}{os.path.basename(rm.ras_gpkg_file)}'
        s3_client.upload_file(Bucket=bucket, Key=out_gpkg_key, Filename=rm.ras_gpkg_file)
        obj = s3_resource.Bucket(bucket).Object(out_gpkg_key)
        meta = get_basic_object_metadata(obj)
        assets['GeoPackage_file'] = {
            'href': f's3://{bucket}/{out_gpkg_key}',
            'meta': meta
            }

        # Overwrite some asset data with S3 metadata
        for s3_asset in assets:
            title = s3_asset.split('/')[-1].replace(' ', '_')
            meta = assets[s3_asset]
            if not title in stac.assets:
                # Make a new asset
                stac.assets[title] = make_stac_assets([s3_asset], bucket=bucket)[title]
            else:
                # replace basic object metadata
                stac.assets[title].href = s3_asset
                for k, v in meta.items():
                    stac.assets[title].extra_fields[k] = v

        # # Export
        # with open(rm.model_stac_json_file, "w") as dst:
        #     dst.write(json.dumps(stac.to_dict()))

        # Move and cleanup
        out_stac_key = f'ebfedata-derived/stac/v{ripple1d.__version__}-rc/{prefix.replace('ebfedata/', '')}{os.path.basename(rm.model_stac_json_file)}'
        # TODO: write bytes to bucket
        s3_client.upload_file(Bucket=bucket, Key=out_stac_key, Filename=rm.model_stac_json_file)

    # return {{"item": {"href"},"png":{}}, "gpkg"}


def run_all():
    logging.info('Initializing conversion process')
    # Get all ras .prj keys
    with open(BLE_JSON_PATH) as in_file:
        ble_json = json.load(in_file)
    # check_for_hash_collisions(ble_json)

    # DEBUGGING.  Test subset
    test_dir = 'ebfedata/12040101_WestForkSanJacinto/Caney Creek-Lake Creek/'
    ble_json = {i: ble_json[i] for i in ble_json if i[:len(test_dir)] == test_dir}

    # Iterate through keys and check for existence
    out_dict = dict()
    t1 = time.perf_counter()
    for ind, f in enumerate(ble_json):

        # Status printing
        if ind % 10 == 0:
            total_time = (time.perf_counter() - t1)
            rate = total_time / (ind + 1)
            print(f'{ind} / {len(ble_json)}  |  Total time: {round(total_time, 1)} seconds  |  Rate: {round(rate, 5)} seconds per requests')

        # Process key
        try:
            key = ble_json[f]['key']
            crs = ble_json[f]['best_crs']
            process_key(key, crs)
            tmp_meta = {
                'key': key,
                'has_error': False,
                'error_str': None
            }
        except Exception as e:
            print(f'Error on {f}')
            print(e)
            tmp_meta = {
                'key': key,
                'has_error': True,
                'error_str': str(e)
            }

        
        # Log meta
        out_dict[key] = tmp_meta

    # Log meta
    out_meta = "production/aws2stac/conversion_meta.json"
    with open(out_meta, mode='w') as out_file:
        json.dump(out_dict, out_file, indent=4)

    # Cleanup
    print('='*50)
    print('Done')
    errors = [out_dict[k]['error_str'] for k in out_dict.keys() if out_dict[k]['has_error']]
    print(f'{len(errors)} keys had errors')
    for e in errors:
        print(e)


# if __name__ == '__main__':
#     # Paths etc
#     BLE_JSON_PATH = "production/aws2stac/crs_inference_ebfe.json"
#     bucket = 'fim'

#     WORKING_DIR = os.path.abspath("tmp_aws2stac")
#     os.makedirs(WORKING_DIR, exist_ok=True)

#     # Helper functions
    


#     run_all()
