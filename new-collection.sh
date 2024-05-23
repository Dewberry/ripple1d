#!/bin/bash
    
# Output from MCAT-RAS / RAS-STAC
BUCKET_NAME=fim
BUCKET_PREFIX=stac/12040101

# Collection info
COLLECTION_ID=mip-data-processing-demo
DESCRIPTION="Demo catalog for ${COLLECTION_ID}"
TITLE="(Preview) FEMA Model Inventory Platform (MIP) HEC-RAS Catalog"

# Load preliminary items to STAC-API
# python -m ripple.stacio.ras_items_to_mip_collection --collection_id ${COLLECTION_ID} --bucket_name ${BUCKET_NAME} \
#   --bucket_prefix ${BUCKET_PREFIX} --description "${DESCRIPTION}" --title "${TITLE}"

# # Update preliminary items for ripple
# python -m ripple.stacio.update_mip_collection_for_ripple --collection_id ${COLLECTION_ID} 

# # Conflate Models
python -m ripple.examples.conflate_with_stac --collection_id ${COLLECTION_ID} 
