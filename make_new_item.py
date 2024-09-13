import inspect
import sys
import logging
from ripple1d.utils.s3_utils import list_keys
from ripple1d.ops.stac_item import process_model
import warnings

warnings.filterwarnings("ignore")

from papipyplug import parse_input, print_results, plugin_logger

PLUGIN_PARAMS = {"required": ["keys", "crs", "model_source"], "optional": ["bucket"]}        
    
if __name__ == "__main__":
    plugin_logger()

    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    keys = input_params.get("keys")
    crs = input_params.get("crs")
    model_source = input_params.get("model_source")

    results = process_model(keys, crs, model_source)
    print_results(results)


