"""
sample command (Harvard Library)
python3 collectionCount.py --p "Harvard Library"

{
    provider:
    providerItemCount:
    providerCollectionCount:
    noCollectionItemCount:
    collections: [
        {
            collectionTitle: title,
            itemsCount: count
        },
        ...
    ]
}

{
    provider:
    itemsInCollections:[
        {itemID:[collectionTitle]},
        ...
    ]
}

{
    provider:
    noDataProviderItemCount:
    dataProviderCount:
    dataProviders: [
        {
            dataProvider: name,
            itemsCount: count
        },
        ...
    ]
}
"""

import dpla_utils
import json
import argparse
import profiler_config as config
from collections import Counter
import logging

DPLA_PROP_providerName = "provider.name"
DPLA_PROP_sourceResource = "sourceResource"
DPLA_PROP_collection = "collection"
DPLA_PROP_title = "title"
DPLA_PROP_dataProvider = "dataProvider"

CCD_PROP_LBL_pageSize = "page_size"
CCD_PROP_LBL_page = "page"
CCD_PROP_LBL_id = "id"
CCD_PROP_LBL_provider = "provider"
CCD_PROP_LBL_itemCount = "providerItemCount"
CCD_PROP_LBL_collectionCount = "providerCollectionCount"
CCD_PROP_LBL_noCollectionItemCount = "noCollectionItemCount"
CCD_PROP_LBL_dataProviderCount = "dataProviderCount"
CCD_PROP_LBL_collections = "collections"
CCD_PROP_LBL_collectionTitle = "collectionTitle"
CCD_PROP_LBL_itemsCount = "itemsCount"
CCD_PROP_LBL_itemsInCollections = "itemsInCollections"
CCD_PROP_LBL_noDataProviderItemCount = "noDataProviderItemCount"
CCD_PROP_LBL_dataProviders = "dataProviders"

FILE_SUFFIX_provider_coll_info = "-providerColl.json"
FILE_SUFFIX_provider_item_info = "-providerItem.json"
FILE_SUFFIX_provider_dp_info = "-providerDP.json"
FILE_path = "output/"

def getCollsByProvider(providerName):
    logger.info("START === provider name: " + providerName)
    CCD_OBJ_provider_coll_parent = {}
    CCD_OBJ_provider_item_parent = {}
    CCD_OBJ_provider_dp_parent = {}
    collList = []
    dataProviderList = []
    items = {}

    page_size = 500
    condition = {DPLA_PROP_providerName: providerName}
    count_item = dpla_utils.dpla_get_count(config.CP_REMOTE, api_key=config.API_KEY, **condition)
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_provider] = providerName
    CCD_OBJ_provider_item_parent[CCD_PROP_LBL_provider] = providerName
    CCD_OBJ_provider_dp_parent[CCD_PROP_LBL_provider] = providerName
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_itemCount] = count_item
    noCollectionItemCount = 0
    noDataProviderItemCount = 0
    logger.info("total count:" + str(count_item))
    total_num_pages = int(count_item / page_size) + 1
    logger.info("total pages: " + str(total_num_pages))
    for i in range(1, total_num_pages + 1, 1):
        condition[CCD_PROP_LBL_pageSize] = page_size
        condition[CCD_PROP_LBL_page] = i
        logger.info("current page: " + str(i))
        response = dpla_utils.dpla_fetch(api_key=config.API_KEY, count=1000, **condition)
        for doc in response:
            dpCount = get_item_volume(DPLA_PROP_dataProvider, doc)
            if dpCount > 0:
                if isinstance(doc[DPLA_PROP_dataProvider], list):
                    for dp in doc[DPLA_PROP_dataProvider]:
                        dataProviderList.append(dp)
                else:
                    dataProviderList.append(doc[DPLA_PROP_dataProvider])
            else:
                noDataProviderItemCount += 1
                logger.info("ITEM WITHOUT DP: " + str(doc[CCD_PROP_LBL_id]))

            sourceresource = doc[DPLA_PROP_sourceResource]
            collectionCount = get_item_volume(DPLA_PROP_collection, sourceresource)
            if collectionCount > 0:
                if isinstance(sourceresource[DPLA_PROP_collection], list):
                    for coll in sourceresource[DPLA_PROP_collection]:
                        collTitleList = []
                        titleCount = get_item_volume(DPLA_PROP_title, coll)
                        if titleCount > 0:
                            if isinstance(coll[DPLA_PROP_title], list):
                                for title in coll[DPLA_PROP_title]:
                                    collList.append(title)
                                    collTitleList.append(title)
                            else:
                                collList.append(coll[DPLA_PROP_title])
                                collTitleList.append(coll[DPLA_PROP_title])
                        else:
                            logger.info("ITEM WITHOUT TITLE:"
                                        + str(doc[CCD_PROP_LBL_id]))
                    items[doc[CCD_PROP_LBL_id]] = collTitleList
                else:
                    title = sourceresource[DPLA_PROP_collection][DPLA_PROP_title]
                    collList.append(title)
                    items[doc[CCD_PROP_LBL_id]] = title
            else:
                noCollectionItemCount += 1

    CCD_OBJ_provider_item_parent[CCD_PROP_LBL_itemsInCollections] = items
    provider_filename = providerName.replace(' ','-') + FILE_SUFFIX_provider_item_info
    write_file(FILE_path + provider_filename, CCD_OBJ_provider_item_parent)

    collectionDict = dict(Counter(collList))
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_collectionCount] = len(collectionDict)

    collections = []
    for collTitle, itemCount in collectionDict.items():
        collection = {}
        collection[CCD_PROP_LBL_collectionTitle] = collTitle
        collection[CCD_PROP_LBL_itemsCount] = itemCount
        collections.append(collection)

    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_collections] = collections
    CCD_OBJ_provider_coll_parent[CCD_PROP_LBL_noCollectionItemCount] = noCollectionItemCount
    provider_filename = providerName.replace(' ','-') + FILE_SUFFIX_provider_coll_info
    write_file(FILE_path + provider_filename, CCD_OBJ_provider_coll_parent)

    dataProviderDict = dict(Counter(dataProviderList))
    CCD_OBJ_provider_dp_parent[CCD_PROP_LBL_dataProviderCount] = len(dataProviderDict)

    dataProviders = []
    for name, itemCount in dataProviderDict.items():
        dp = {}
        dp[DPLA_PROP_dataProvider] = name
        dp[CCD_PROP_LBL_itemsCount] = itemCount
        dataProviders.append(dp)

    CCD_OBJ_provider_dp_parent[CCD_PROP_LBL_noDataProviderItemCount] = noDataProviderItemCount
    CCD_OBJ_provider_dp_parent[CCD_PROP_LBL_dataProviders] = dataProviders
    provider_filename = providerName.replace(' ','-') + FILE_SUFFIX_provider_dp_info
    write_file(FILE_path + provider_filename, CCD_OBJ_provider_dp_parent)
    logger.info("END === provider name: " + providerName)

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
"""
# call the profiler and let it do its magic
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler

handler = logging.FileHandler('Log.log')
handler.setLevel(logging.INFO)

# create a logging format

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger

logger.addHandler(handler)

providers = ['HathiTrust', 'National Archives and Records Administration', 'Smithsonian Institution', 'The New York Public Library', 'University of Southern California. Libraries', 'The Portal to Texas History', 'Mountain West Digital Library', 'California Digital Library', 'Minnesota Digital Library', 'Digital Library of Georgia', 'Recollection Wisconsin', 'Empire State Digital Network', 'North Carolina Digital Heritage Center', 'Digital Commonwealth', 'Internet Archive', 'Missouri Hub', 'PA Digital', 'United States Government Publishing Office (GPO)', 'Digital Library of Tennessee', 'Indiana Memory', 'University of Washington', 'Kentucky Digital Library', 'Biodiversity Heritage Library', 'South Carolina Digital Library', 'ARTstor', 'J. Paul Getty Trust', 'David Rumsey', 'University of Virginia Library', 'University of Illinois at Urbana-Champaign', 'Harvard Library']
for provider_name in providers:
    getCollsByProvider(provider_name)
