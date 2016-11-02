"""
sample command (Harvard Library)
python3 collectionCount.py --p "Harvard Library"
"""

import dpla_utils
import json
import argparse
import profiler_config as config
from collections import Counter

DPLA_PROP_providerName = "provider.name"
DPLA_PROP_sourceResource = "sourceResource"
DPLA_PROP_collection = "collection"
DPLA_PROP_title = "title"

CCD_PROP_LBL_totalItems = "totalItems"
CCD_PROP_LBL_pageSize = "page_size"
CCD_PROP_LBL_page = "page"
CCD_PROP_LBL_collectionCount = "collectionCount"
CCD_PROP_LBL_averageCollection = "averageCollection"

FILE_SUFFIX_provider_coll_info = "-providerColl.json"
FILE_path = "output/"

def getCollsByProvider(providerName):
    CCD_OBJ_provider_coll_parent = {}
    page_size = 500
    collList = []
    condition = {DPLA_PROP_providerName: providerName}
    count_item = dpla_utils.dpla_get_count(config.CP_REMOTE, api_key=config.API_KEY, **condition)
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_totalItems] = count_item
    total_num_pages = int(count_item / page_size) + 1
    print("total pages: " + str(total_num_pages))
    for i in range(1, total_num_pages + 1, 1):
        condition[CCD_PROP_LBL_pageSize] = page_size
        condition[CCD_PROP_LBL_page] = i
        print("current page: " + str(i))
        response = dpla_utils.dpla_fetch(api_key=config.API_KEY, count=10, **condition)
        for doc in response:
            sourceresource = doc[DPLA_PROP_sourceResource]
            collectionCount = get_item_volume(DPLA_PROP_collection, sourceresource)
            if collectionCount > 0:
                if collectionCount == 1:
                    collList.append(sourceresource[DPLA_PROP_collection][DPLA_PROP_title])
                else:
                    for coll in sourceresource[DPLA_PROP_collection]:
                        collList.append(coll[DPLA_PROP_title])

    print("end...")
    counts = dict(Counter(collList))
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_collectionCount] = counts
    averageCount = len(collList)/len(counts)
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_averageCollection] = averageCount
    provider_filename = providerName.replace(' ','-') + FILE_SUFFIX_provider_coll_info
    write_file(FILE_path + provider_filename, CCD_OBJ_provider_coll_parent)

# get the volume for the filed in the location
def get_item_volume(field, location):
    assert isinstance(location, object)
    if field in location:
        field_obj = location[field]
        if isinstance(field_obj, list):
            return len(field_obj)
        else:
            return 1
    else:
        return 0

def write_file(filename, json_object):
    output = open(filename, 'w')
    output.write(json.dumps(json_object))
    output.close()

"""
Begin code execution here
# """

# setup argument parser
parser = argparse.ArgumentParser(description='please insert parameters')
parser.add_argument("-p", "--provider", help="please identify provider's name")
# parse arguments received from the command line
args = parser.parse_args()

provider_name = ""
if args.provider:
   provider_name = args.provider

#call the profiler and let it do its magic
getCollsByProvider(provider_name)
