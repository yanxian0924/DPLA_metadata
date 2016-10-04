__author__ = 'zhangzhang'
"""
Retrieves data from the DPLA and extracts characteristics such as the providers, collections, data providers, and relationships between the items and the collections

all data created by this program is saved in json files

sample command: python3 dpla_profiler.py [-f <filename>]
"""
from pprint import pprint
import dpla_utils
import sys
import json
import argparse
import collections
import profiler_config as config
import time
from os.path import expanduser

'''
Constants and variables for this program
'''

# File paths and names

FILENAME_dpla_data_output = "dpla-base-data.json"
# the prefix for collection-level files is the value of the value 
# of the provider.name property with hyphens replacing the spaces 
FILE_SUFFIX_provider_data = "-provderInfo.json"
FILE_SUFFIX_coll_data = "-collInfo.json"
FILE_SUFFIX_multiColl_item_data = "-multiCollInfo.json"

# CCD data object and property labels
CCD_OBJ_LBL_adminDetails = "adminDetails"
CCD_PROP_LBL_analysisDate = "analysisDate"
CCD_PROP_LBL_analysisTime = "analysisTime"

CCD_OBJ_LBL_dplaData = "dplaData"
CCD_OBJ_LBL_providerData = "providerData"

CCD_PROP_LBL_providerId = "@id"
CCD_PROP_LBL_providerName = "name"
CCD_PROP_LBL_itemCountByName = "itemCountByName"
CCD_PROP_LBL_itemCountById = "itemCountById"
CCD_PROP_LBL_itemsInCollections = "itemsInCollections"
CCD_PROP_LBL_collectionCount = "collectionCount"
CCD_PROP_LBL_dataProviderCount = "dataProviderCount"
CCD_PROP_LBL_providerCountByName = "providerCountByName"
CCD_PROP_LBL_providerCountById = "providerCountById"
CCD_PROP_LBL_missingItemsByName = "missingItemsByName"
CCD_PROP_LBL_missingItemsById = "missingItemsById"

CCD_PROP_LBL_itemCount = "itemCount"
CCD_PROP_LBL_dataProviders = "dataProviders"

# DPLA property names
DPLA_PROP_dataProvider = "dataProvider"
DPLA_PROP_providerName = "provider.name"
DPLA_PROP_providerId = "provider.@id"
DPLA_PROP_itemId = "id"
DPLA_PROP_collectionId = "sourceResource.collection.id"
DPLA_PROP_collectionTitle = "sourceResource.collection.title"

DPLA_API_QUERY_facets = "facets"
DPLA_API_PROP_total = "total"
DPLA_API_PROP_missing = "missing"
DPLA_API_PROP_terms = "terms"
DPLA_API_PROP_term = "term"
DPLA_API_PROP_count = "count"

def profile_dpla(base_dpla_filename):

    # create base object to store the base dpla-level data
    CCD_OBJ_dpla_parent = {}

    # create the admin details object
    CCD_OBJ_adminDetails = {}
    CCD_OBJ_adminDetails[CCD_PROP_LBL_analysisDate] = time.strftime("%d/%m/%Y")
    CCD_OBJ_adminDetails[CCD_PROP_LBL_analysisTime] = time.strftime("%H:%M:%S")

    # add adminDetails to the root json structure in the dpla base data file
    CCD_OBJ_dpla_parent[CCD_OBJ_LBL_adminDetails] = CCD_OBJ_adminDetails

    # create the object to store the dpla-level data
    CCD_OBJ_dplaData = {}

    # retrieve the number of items in collections
    query_items_in_collections = {DPLA_API_QUERY_facets: DPLA_PROP_collectionId}
    response_provider_facets = dpla_utils.dpla_fetch_facets_remote( \
    api_key=config.API_KEY, **query_items_in_collections)

    CCD_OBJ_dplaData[CCD_PROP_LBL_itemsInCollections] = \
      response_provider_facets[DPLA_PROP_collectionId][DPLA_API_PROP_total]

    # retrieve from the DPLA and store provider-related data
    query_provider_facets = {DPLA_API_QUERY_facets: DPLA_PROP_providerName \
                               + "," + DPLA_PROP_providerId}
    response_provider_facets = dpla_utils.dpla_fetch_facets_remote( \
    api_key=config.API_KEY, **query_provider_facets)

    # retrieve count of total DPLA items by name and id
    CCD_OBJ_dplaData[CCD_PROP_LBL_itemCountByName] = \
      response_provider_facets[DPLA_PROP_providerName][DPLA_API_PROP_total]

    CCD_OBJ_dplaData[CCD_PROP_LBL_itemCountById] = \
      response_provider_facets[DPLA_PROP_providerId][DPLA_API_PROP_total]

    # retrieve missing items by name and id
    CCD_OBJ_dplaData[CCD_PROP_LBL_missingItemsByName] = \
      response_provider_facets[DPLA_PROP_providerName][DPLA_API_PROP_missing]

    CCD_OBJ_dplaData[CCD_PROP_LBL_missingItemsById] = \
      response_provider_facets[DPLA_PROP_providerId][DPLA_API_PROP_missing]

    # retrieve number of missing items by provider name and id
    CCD_OBJ_dplaData[CCD_PROP_LBL_providerCountByName] = \
      len( response_provider_facets[DPLA_PROP_providerName]\
           [DPLA_API_PROP_terms])

    CCD_OBJ_dplaData[CCD_PROP_LBL_providerCountById] = \
      len( response_provider_facets[DPLA_PROP_providerId]\
           [DPLA_API_PROP_terms])

    # for each provider, retrieve and process the data
    for provider in \
    response_provider_facets[DPLA_PROP_providerId][DPLA_API_PROP_terms]:
 
        # create object to store provider data
        CCD_OBJ_provider_parent = {}
        
        # retrieve dataProver details
        query_dataProvider_facets = {DPLA_API_QUERY_facets: \
                                     DPLA_PROP_dataProvider + "," + \
                                     DPLA_PROP_providerName,\
                                     DPLA_PROP_providerId: \
                                     provider[DPLA_API_PROP_term]}
        response_dataProvider_facets = dpla_utils.dpla_fetch_facets_remote( \
        api_key=config.API_KEY, **query_dataProvider_facets)

        #pprint( query_dataProvider_facets)
        #pprint( response_dataProvider_facets)

        # number of items from this provider
        CCD_OBJ_provider_parent[CCD_PROP_LBL_itemCount] = \
                                       provider[DPLA_API_PROP_count]

        #pprint( response_dataProvider_facets[DPLA_PROP_dataProvider][DPLA_API_PROP_terms])
        #pprint( DPLA_PROP_dataProvider)
        #pprint( DPLA_API_PROP_terms)
        # number of data providers providing data through this provider
        CCD_OBJ_provider_parent[CCD_PROP_LBL_dataProviderCount] = len( response_dataProvider_facets[DPLA_PROP_dataProvider][DPLA_API_PROP_terms])

        pprint( CCD_OBJ_provider_parent)

        # save the base dpla data to file
        provider_name = response_dataProvider_facets[DPLA_PROP_providerName][DPLA_API_PROP_terms][0][DPLA_API_PROP_term].replace(' ','-')
        provider_filename = provider_name + FILE_SUFFIX_provider_data
        provider_data_file = open(provider_filename, 'w')
        provider_data_file.write(json.dumps(CCD_OBJ_provider_parent))
        provider_data_file.close()


    # add dpla-level data to the root json structure 
    CCD_OBJ_dpla_parent[CCD_OBJ_LBL_dplaData] = CCD_OBJ_dplaData

    # save the base dpla data to file
    base_dpla_data_file = open(base_dpla_filename, 'w')
    base_dpla_data_file.write(json.dumps(CCD_OBJ_dpla_parent))
    base_dpla_data_file.close()

    sys.exit(0)

    """
    :rtype : object

    """
    id_list = []
    id_duplicated = []
    condition = {'sourceResource.collection.id': collection_id, 'fields':'sourceResource.date.displayDate'}
    count_item = dpla_utils.dpla_get_count(config.CP_REMOTE, api_key=config.API_KEY, **condition)
    print("Total item:", count_item)
    page_size = 500

    total_num_pages = int(count_item / page_size) + 1
    print('Total Pages:', total_num_pages)
    # define the collection that will hold the output object
    collection = collections.OrderedDict()
    sample_collection = dpla_utils.dpla_get_collection_info(config.CP_REMOTE, collection_id=collection_id,
                                                            api_key=config.API_KEY, )
    collection.update({'collection': sample_collection})
    # data_provider_info = {"dataProvider": sample_doc["dataProvider"], "provider": sample_doc["provider"]}
    # collection['collection'].update(data_provider_info)
    # define the counter for usage
    collection_volume_title = 0
    collection_volume_subject = 0
    collection_volume_displaydate = 0
    collection_volume_language = 0
    collection_volume_creator = 0
    collection_volume_publisher = 0
    collection_volume_spatialname = 0
    collection_volume_spatialcoords = 0
    collection_volume_rights = 0
    collection_volume_description = 0
    collection_volume_provider = 0
    collection_volume_dataprovider = 0
    collection_volume_format = 0
    collection_volume_type = 0
    # add counter for collection
    collection_volume_collection = 0
    # define the counter for usage
    collection_usage_title = 0
    collection_usage_subject = 0
    collection_usage_displaydate = 0
    collection_usage_language = 0
    collection_usage_creator = 0
    collection_usage_publisher = 0
    collection_usage_spatialname = 0
    collection_usage_spatialcoords = 0
    collection_usage_rights = 0
    collection_usage_description = 0
    collection_usage_provider = 0
    collection_usage_dataprovider = 0
    collection_usage_format = 0
    collection_usage_type = 0
    # add counter for usage of collection
    collection_usage_collection = 0
    item_detail = []
    # process items, in each time, get 500 items, process them and drop them
    for i in range(1, total_num_pages + 1, 1):
        print('processing page', i)
        condition['page_size'] = page_size
        condition['page'] = i
        dpla_response = dpla_utils.dpla_fetch(api_key=config.API_KEY, count=1000, **condition)
        docs = dpla_response

        for doc in docs:
            id_list.append(doc)


    #             sourceresource = doc['sourceResource']
    #             # compose item_volume section
    #             # add collection information
    #             item_volume = dict(title=get_item_volume('title', sourceresource),
    #                                format=get_item_volume('format', sourceresource),
    #                                type=get_item_volume('type', sourceresource),
    #                                subject=get_item_volume('subject', sourceresource),
    #                                rights=get_item_volume('rights', sourceresource),
    #                                language=get_item_volume('language', sourceresource),
    #                                creator=get_item_volume('creator', sourceresource),
    #                                publisher=get_item_volume('publisher', sourceresource),
    #                                description=get_item_volume('description', sourceresource),
    #                                collection=get_item_volume('collection', sourceresource),
    #                                provider=get_item_volume('provider', doc),
    #                                dataProvider=get_item_volume('dataProvider', doc))
    #             # add spatial to item_volume
    #             if 'spatial' in sourceresource:
    #                 spatials = sourceresource['spatial']
    #                 item_volume['spatialName'] = 0
    #                 item_volume['spatialCoords'] = 0
    #                 for spatial in spatials:
    #                     item_volume['spatialName'] += get_item_volume('name', spatial)
    #                     item_volume['spatialCoords'] += get_item_volume('coordinates', spatial)
    #             else:
    #                 item_volume['spatialName'] = 0
    #                 item_volume['spatialCoords'] = 0
    #
    #             # add displaydate to item_volume
    #             if 'date' in sourceresource:
    #                 date = sourceresource['date']
    #                 item_volume['displayDate'] = get_item_volume('displayDate', date)
    #
    #
    #             else:
    #                 item_volume['displayDate'] = 0
    #             # commented by unmil to improve code run time
		# # `if item_volume['displayDate']<1:
    #             #    print (item_volume['displayDate'])
    #             item['itemVolume'] = item_volume
    #             item_detail.append(item)
    #             # sum each status
    #             collection_volume_title = collection_volume_title + item['itemVolume']['title']
    #             collection_volume_subject = collection_volume_subject + item['itemVolume']['subject']
    #             collection_volume_displaydate = collection_volume_displaydate + item['itemVolume']['displayDate']
    #             collection_volume_language = collection_volume_language + item['itemVolume']['language']
    #             collection_volume_creator = collection_volume_creator + item['itemVolume']['creator']
    #             collection_volume_publisher = collection_volume_publisher + item['itemVolume']['publisher']
    #             collection_volume_rights = collection_volume_rights + item['itemVolume']['rights']
    #             collection_volume_spatialname = collection_volume_spatialname + item['itemVolume']['spatialName']
    #             collection_volume_spatialcoords = collection_volume_spatialcoords + item['itemVolume']['spatialCoords']
    #             collection_volume_description = collection_volume_description + item['itemVolume']['description']
    #             collection_volume_provider = collection_volume_provider + item['itemVolume']['provider']
    #             collection_volume_dataprovider = collection_volume_dataprovider + item['itemVolume']['dataProvider']
    #             collection_volume_format = collection_volume_format + item['itemVolume']['format']
    #             collection_volume_type = collection_volume_type + item['itemVolume']['type']
    #             # add collection
    #             collection_volume_collection = collection_volume_collection + item['itemVolume']['collection']
    #             # sum usage information
    #             collection_usage_title += get_usage(item['itemVolume']['title'])
    #             collection_usage_subject += get_usage(item['itemVolume']['subject'])
    #             collection_usage_displaydate += get_usage(item['itemVolume']['displayDate'])
    #             collection_usage_language += get_usage(item['itemVolume']['language'])
    #             collection_usage_creator += get_usage(item['itemVolume']['creator'])
    #             collection_usage_publisher += get_usage(item['itemVolume']['publisher'])
    #             collection_usage_rights += get_usage(item['itemVolume']['rights'])
    #             collection_usage_spatialcoords += get_usage(item['itemVolume']['spatialName'])
    #             collection_usage_spatialname += get_usage(item['itemVolume']['spatialName'])
    #             collection_usage_description += get_usage(item['itemVolume']['description'])
    #             collection_usage_provider += get_usage(item['itemVolume']['provider'])
    #             collection_usage_dataprovider += get_usage(item['itemVolume']['dataProvider'])
    #             collection_usage_format += get_usage(item['itemVolume']['format'])
    #             collection_usage_type += get_usage(item['itemVolume']['type'])
    #             # add collection
    #             collection_usage_collection += get_usage(collection_volume_collection)
    #
    # collection_volume = {
    #     'title': collection_volume_title,
    #     'format': collection_volume_format,
    #     'type': collection_volume_type,
    #     'subject': collection_volume_subject,
    #     'rights': collection_volume_rights,
    #     'language': collection_volume_language,
    #     'creator': collection_volume_creator,
    #     'publisher': collection_volume_publisher,
    #     'description': collection_volume_description,
    #     'provider': collection_volume_provider,
    #     'dataProvider': collection_volume_dataprovider,
    #     'displayDate': collection_volume_displaydate,
    #     'spatialCoords': collection_volume_spatialcoords,
    #     'spatialName': collection_volume_spatialname,
    #     'collection': collection_volume_collection
    # }
    # collection_usage = {
    #     'title': collection_usage_title,
    #     'format': collection_usage_format,
    #     'type': collection_usage_type,
    #     'subject': collection_usage_subject,
    #     'rights': collection_usage_rights,
    #     'language': collection_usage_language,
    #     'creator': collection_usage_creator,
    #     'publisher': collection_usage_publisher,
    #     'description': collection_usage_description,
    #     'provider': collection_usage_provider,
    #     'dataProvider': collection_usage_dataprovider,
    #     'displayDate': collection_usage_displaydate,
    #     'spatialCoords': collection_usage_spatialcoords,
    #     'spatialName': collection_usage_spatialname,
    #     'collection': collection_usage_collection
    # }
    #
    # coll_metadata_detail = collections.OrderedDict()
    # coll_metadata_detail['itemCount'] = len(item_detail)
    # coll_metadata_detail['dateProfiled'] = time.strftime("%d/%m/%Y")
    # if config.CP_REMOTE:
    #     coll_metadata_detail['Source'] = "Remote"
    # else:
    #     coll_metadata_detail['Source'] = 'Local'
    #
    # coll_metadata_detail['collectionVolume'] = collection_volume
    # coll_metadata_detail['collectionUsage'] = collection_usage
    #
    # # put the metadataDetail to the collection
    # collection['collMetadataDetail'] = coll_metadata_detail
    # collection['duplicated_item'] = id_duplicated
    # # put itemDetail to collection
    # if not collection_only:
    #     collection['itemDetail'] = item_detail
    # write the json format fo the data to local file named by user
    dest_file = open(filename, 'w')
    dest_file.write(json.dumps(id_list))
    dest_file.close()


# return 1 if the collection volume field>0, if filed=0 mean there is no usage of this field
def get_usage(collection_volume_field):
    if collection_volume_field > 0:
        return 1
    else:
        return 0


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


def have_or_not(a):
    if a > 0:
        return 1
    else:
        return 0


"""
Begin code execution here
# """

# setup argument parser
parser = argparse.ArgumentParser(description='please insert parameters')
parser.add_argument("-f", "--filename",
                    help="the location for storing the dpla statistics." +\
                    " If not set, these statistics are stored in: " + \
                    FILENAME_dpla_data_output)

# parse arguments received from the command line
args = parser.parse_args()

dpla_data_output_filename = FILENAME_dpla_data_output
#set the output filename
if args.filename:
   dpla_data_output_filename = args.filename

#call the profiler and let it do its magic
profile_dpla(dpla_data_output_filename)
