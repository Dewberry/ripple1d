import json
import time
import os
import shutil
import hashlib
import re
import glob
import logging
from datetime import datetime

from ripple1d.utils.s3_utils import *
from ripple1d.ras_to_gpkg import gpkg_from_ras, geom_flow_to_gpkg, RasProject
from ripple1d.ops.stac_item import rasmodel_to_stac
from ripple1d.data_model import RasModelStructure, RippleSourceModel
from ripple1d.utils.s3_utils import get_basic_object_metadata
from ripple1d.utils.ripple_utils import prj_is_ras
from ripple1d.ops.stac_item import make_stac_assets


BLE_JSON_PATH = "production/aws2stac/crs_inference_ebfe.json"
OUTPUT_DIR = os.path.abspath("production/aws2stac/items")
os.makedirs(OUTPUT_DIR, exist_ok=True)
BUCKET = 'fim'
RELEVANT_FILE_FINDER = re.compile(r'\.[Pp][Rr][Jj]|\.[Gg]\d{2}|\.[Pp]\d{2}|\.[FfUuQq]\d{2}')
file_handler = datetime.now().strftime('production/aws2stac/conversion_log_%H_%M_%d_%m_%Y.log')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.basicConfig(handlers=[logging.FileHandler(file_handler), console_handler], encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')


def download_model(s3_access, assets, tmp_dir):
    # Download all ras files in the directory of .prj file
    logging.info(f'Downloading assets: {assets}')
    download_keys = [s for s in assets.keys() if RELEVANT_FILE_FINDER.fullmatch(pathlib.Path(s).suffix)]
    out_paths = list()
    for k in download_keys:
        fname = k.split('/')[-1]
        save_path = os.path.join(tmp_dir, fname)
        out_paths.append(fname)
        s3_access['s3_client'].download_file(BUCKET, k, save_path)
    return out_paths

def get_assets(s3_access, key):
    # find all files on same level as key and get metadata
    prefix = '/'.join(key.split('/')[:-1]) + '/'
    logging.info(f'Finding assets associated with prefix {prefix}')
    keys = list_keys(s3_access['s3_client'], bucket=BUCKET, prefix=prefix)
    asset_dict = dict()
    for k in keys:
        obj = s3_access['s3_resource'].Bucket(BUCKET).Object(k)
        meta = get_basic_object_metadata(obj)
        asset_dict[k] = meta
    return prefix, asset_dict


def process_key(s3_access, key, crs):
    """Converts RAS model associated with a .prj S3 key to stac"""
    logging.info(f'Processing key: {key}')

    # Make a temp folder
    tmp_hash = hashlib.sha1(bytes(key, encoding="utf-8")).hexdigest()
    tmp_dir = os.path.join(os.getcwd(), 'tmp_processing', tmp_hash)
    os.makedirs(tmp_dir, exist_ok=True)

    # Get assets and Download model
    prefix, assets = get_assets(s3_access, key)
    download_model(s3_access, assets, tmp_dir)

    # Find ras .prj file
    prjs = glob.glob(f"{tmp_dir}/*.prj")
    prjs = [prj for prj in prjs if prj_is_ras(prj)]
    if len(prjs) > 1:
        logging.warning(f'More than one ras .prj found for {key}')
    ras_prj_path = prjs[-1]

    # Make a geopackage
    logging.info(f'Making geopackage for {key}')
    rp = RasProject(ras_prj_path)
    ras_gpkg_path = ras_prj_path.replace(".prj", ".gpkg")
    geom_flow_to_gpkg(rp, crs, ras_gpkg_path, dict())

    # Create a STAC item
    logging.info(f'Making stac item for {key}')
    rm = RippleSourceModel(ras_prj_path, crs)
    stac = rasmodel_to_stac(rm, prefix)

    # Overwrite with full set of assets
    new_assets = make_stac_assets(assets, bucket=BUCKET)
    stac.assets = new_assets
    with open(rm.model_stac_json_file, "w") as dst:
        dst.write(json.dumps(stac.to_dict()))

    # Move and cleanup
    new_stac = os.path.join(OUTPUT_DIR, os.path.basename(rm.model_stac_json_file))
    shutil.move(rm.model_stac_json_file, new_stac)
    meta['stac_url'] = new_stac
    new_thumb = os.path.join(OUTPUT_DIR, os.path.basename(rm.thumbnail_png))
    shutil.move(rm.thumbnail_png, new_thumb)
    meta['png_url'] = new_thumb
    shutil.rmtree(tmp_dir)

    return meta


def check_for_hash_collisions(json):
    hashes = set([hashlib.sha1(bytes(k, encoding="utf-8")).hexdigest() for k in json])
    assert len(hashes) == len(json), 'Hash collision found in source json'


def run_all():
    logging.info('Initializing conversion process')
    # Get all ras .prj keys
    with open(BLE_JSON_PATH) as in_file:
        ble_json = json.load(in_file)
    # check_for_hash_collisions(ble_json)
    
    # Initialize s3 access
    s3 = dict()
    s3['session'], s3['s3_client'], s3['s3_resource'] = init_s3_resources()

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
            process_key(s3, key, crs)
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


if __name__ == '__main__':
    run_all()
