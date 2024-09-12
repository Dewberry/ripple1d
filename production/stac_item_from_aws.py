import glob
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import time

from ripple1d import __version__ as version
from ripple1d.ras_to_gpkg import gpkg_from_ras
from ripple1d.ops.stac_item import rasmodel_to_stac,make_stac_assets
from ripple1d.data_model import RippleSourceModel
from ripple1d.utils.s3_utils import get_basic_object_metadata, init_s3_resources, list_keys
from ripple1d.utils.ripple_utils import prj_is_ras


def download_model(s3_client, bucket: str, keys: list, tmp_dir: str) -> None:
    """Download all ras files in the directory of .prj file."""
    logging.debug(f'Downloading files: {keys}')
    RELEVANT_FILE_FINDER = re.compile(r'\.[Pp][Rr][Jj]|\.[Gg]\d{2}|\.[Pp]\d{2}|\.[FfUuQq]\d{2}')
    for key in keys:
        if RELEVANT_FILE_FINDER.fullmatch(Path(key).suffix):
            s3_client.download_file(bucket, key, os.path.join(tmp_dir, Path(key).name))

def upload_file(bucket, s3_client, s3_resource, basename: str, file: str | dict, type: str, prefix: str, public: bool) -> dict:
    """Upload file to correct spot on S3"""
    out_key = f'ebfedata-derived/{type}/v{version}-rc/{prefix.replace('ebfedata/', '')}{basename}'
    if isinstance(file, str):
        s3_client.upload_file(Bucket=bucket, Key=out_key, Filename=file)
    elif isinstance(file, dict):
        s3_client.put_object(Bucket=bucket, Key=out_key, Body=json.dumps(file).encode())

    obj = s3_resource.Bucket(bucket).Object(out_key)
    meta = get_basic_object_metadata(obj)

    if public:
        return {'href': f'{bucket}.amazonaws.com/{out_key}', 'meta': meta}
    else:
        return{'href': f's3://{bucket}/{out_key}', 'meta': meta}


def get_assets(s3_resource, bucket, keys)-> dict:
    """Get metadata for a list of S3 keys"""
    asset_dict = {}
    for key in keys:
        obj = s3_resource.Bucket(bucket).Object(key)
        asset_dict[key] = {
            'href': f's3://{bucket}/{key}',
            'meta': get_basic_object_metadata(obj)
            }
    return asset_dict


def process_key(bucket:str, key:str, crs:str) -> dict:
    """Convert RAS model associated with a .prj S3 key to stac"""
    logging.info(f'Processing key: {key}')

    _, s3_client, s3_resource = init_s3_resources()

    # Get assets and Download model
    logging.info(f'Finding assets associated with prefix {key}')
    prefix = '/'.join(key.split('/')[:-1]) + '/'
    keys = list_keys(s3_client, bucket, prefix)
    assets = get_assets(s3_resource, bucket, keys)

    with tempfile.TemporaryDirectory() as tmp_dir:
        logging.debug(f'Temp folder created at {tmp_dir}')

        download_model(s3_client, bucket, assets, tmp_dir)

        # Make a geopackage
        logging.info(f'Making geopackage for {key}')
        gpkg_from_ras(tmp_dir, crs, {})

        # Find ras .prj file, make instance of RippleSourceModel, and convert to stac
        prjs = glob.glob(f"{tmp_dir}/*.prj")
        prjs = [prj for prj in prjs if prj_is_ras(prj)]
        if len(prjs) != 1:
            raise KeyError(f"Expected 1 RAS file, found {len(prjs)}: {prjs}")
        else:
            ras_prj_path = prjs[0]
        rm = RippleSourceModel(ras_prj_path, crs)
        logging.info(f'Making stac item for {key}')
        stac = rasmodel_to_stac(rm, save_json=False)

        # Upload png and gpkg to s3
        assets['Thumbnail'] = upload_file(bucket, s3_client, s3_resource, os.path.basename(rm.thumbnail_png), rm.thumbnail_png, 'stac', prefix, public=True)
        assets['GeoPackage_file'] = upload_file(bucket, s3_client, s3_resource, os.path.basename(rm.ras_gpkg_file), rm.ras_gpkg_file, 'gpkgs', prefix, public=False)

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
        
        # Export
        assets['stac_item'] = upload_file(bucket, s3_client, s3_resource, os.path.basename(rm.model_stac_json_file), stac.to_dict(), 'stac', prefix, public=True)

    return {
        "stac_item": {"href": assets['stac_item']['href']}, 
        "thumbnail": {"href": assets['Thumbnail']['href']}, 
        "gpkg": {"href": assets['GeoPackage_file']['href']}
        }



def run_all():
    logging.info('Initializing conversion process')

    # Paths etc
    ble_json_path = "production/aws2stac/crs_inference_ebfe.json"
    bucket = 'fim'

    # Get all ras .prj keys
    with open(ble_json_path) as in_file:
        ble_json = json.load(in_file)

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
            process_key(bucket, key, crs)
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
