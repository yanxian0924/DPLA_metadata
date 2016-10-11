__author__ = 'Yanxian Liu'
"""
Retrieves sourceresource data from the DPLA according to a specific data provider

sample command: python3 dpla_dataProvider.py [-n <dataProvider name>] [-f <filename>]

JSON schema
{
  "dataProviderName": "",
  "itemCount": 500,
  "sourceResource":{
    "collection": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "contributor": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "creator": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "date": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "description": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "format": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "genre": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "identifier": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "language": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "place": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "subject": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "title": {
      "usage":100,
      "volume":100,
      "value":[]
    },
    "type": {
      "usage":100,
      "volume":100,
      "value":[]
    }
  },
  "provider":{
    "usage":100,
    "volume":100,
    "value":[]
  }
}
"""
import sys
import json
import dpla_utils
import profiler_config as config
import argparse

from pprint import pprint
'''
Constants and variables for this program
'''
FILENAME_dpla_data_output = "dpla-dataProvider-data.json"

# DPLA_API_QUERY_facets = "facets"
DPLA_PROP_dataProvider = "dataProvider"

def dataProvider(dataProvider_name, base_dpla_filename):
    # retrieve from the DPLA and store dataProvider-related data
    query_dataProvider = {DPLA_PROP_dataProvider: \
                          dataProvider_name,
                          "exact_field_match":"true"}
    response_dataProvider = dpla_utils.dpla_fetch_remote( \
                            api_key=config.API_KEY, **query_dataProvider)
    docs = response_dataProvider

    # item count
    item_count = 0

    # define the counter for data volume
    volume_collection = 0
    volume_contributor = 0
    volume_creator = 0
    volume_displayDate = 0
    volume_description = 0
    volume_format = 0
    volume_genre = 0
    volume_identifier = 0
    volume_language = 0
    volume_place = 0
    volume_subject = 0
    volume_title = 0
    volume_type = 0
    volume_provider = 0

    # define the counter for usage
    usage_collection = 0
    usage_contributor = 0
    usage_creator = 0
    usage_displayDate = 0
    usage_description = 0
    usage_format = 0
    usage_genre = 0
    usage_identifier = 0
    usage_language = 0
    usage_place = 0
    usage_subject = 0
    usage_title = 0
    usage_type = 0
    usage_provider = 0

    # define set for data value
    value_collection = []
    value_contributor = []
    value_creator = []
    value_displayDate = []
    value_description = []
    value_format = []
    value_genre = []
    value_identifier = []
    value_language = []
    value_place = []
    value_subject = []
    value_title = []
    value_type = []
    value_provider = []

    for doc in docs:
        sourceresource = doc['sourceResource']
        # compose item_volume section
        # add collection information
        item_volume = dict(collection=get_item_volume('collection', sourceresource),
                           contributor=get_item_volume('contributor', sourceresource),
                           creator=get_item_volume('creator', sourceresource),
                           description=get_item_volume('description', sourceresource),
                           format=get_item_volume('format', sourceresource),
                           genre=get_item_volume('genre', sourceresource),
                           identifier=get_item_volume('identifier', sourceresource),
                           language=get_item_volume('language', sourceresource),
                           place=get_item_volume('place', sourceresource),
                           subject=get_item_volume('subject', sourceresource),
                           title=get_item_volume('title', sourceresource),
                           type=get_item_volume('type', sourceresource),
                           provider=get_item_volume('provider', doc))

        # add displaydate to item_volume
        if 'date' in sourceresource:
            date = sourceresource['date']
            item_volume['displayDate'] = get_item_volume('displayDate', date)
        else:
            item_volume['displayDate'] = 0

        item_count += 1

        # sum each status
        volume_collection += item_volume['collection']
        volume_contributor += item_volume['contributor']
        volume_creator += item_volume['creator']
        volume_displayDate += item_volume['displayDate']
        volume_description += item_volume['description']
        volume_format += item_volume['format']
        volume_genre += item_volume['genre']
        volume_identifier += item_volume['identifier']
        volume_language += item_volume['language']
        volume_place += item_volume['place']
        volume_subject += item_volume['subject']
        volume_title += item_volume['title']
        volume_type += item_volume['type']
        volume_provider += item_volume['provider']

        # sum usage information
        usage_collection += get_usage(item_volume['collection'])
        usage_contributor += get_usage(item_volume['contributor'])
        usage_creator += get_usage(item_volume['creator'])
        usage_displayDate += get_usage(item_volume['displayDate'])
        usage_description += get_usage(item_volume['description'])
        usage_format += get_usage(item_volume['format'])
        usage_genre += get_usage(item_volume['genre'])
        usage_identifier += get_usage(item_volume['identifier'])
        usage_language += get_usage(item_volume['language'])
        usage_place += get_usage(item_volume['place'])
        usage_subject += get_usage(item_volume['subject'])
        usage_title += get_usage(item_volume['title'])
        usage_type += get_usage(item_volume['type'])
        usage_provider += get_usage(item_volume['provider'])

        # add actual values
        if "collection" in sourceresource:
            value_collection.append(sourceresource["collection"])

        if "contributor" in sourceresource:
            value_contributor.append(sourceresource["contributor"])

        if "creator" in sourceresource:
            value_creator.append(sourceresource["creator"])

        if "displayDate" in sourceresource:
            value_date.append(sourceresource["date"])

        if "description" in sourceresource:
            value_description.append(sourceresource["description"])

        if "format" in sourceresource:
            value_format.append(sourceresource["format"])

        if "genre" in sourceresource:
            value_genre.append(sourceresource["genre"])

        if "identifier" in sourceresource:
            value_identifier.append(sourceresource["identifier"])

        if "language" in sourceresource:
            value_language.append(sourceresource["language"])

        if "place" in sourceresource:
            value_place.append(sourceresource["place"])

        if "subject" in sourceresource:
            value_subject.append(sourceresource["subject"])

        if "title" in sourceresource:
            value_title.append(sourceresource["title"])

        if "provider" in doc:
            value_provider.append(doc["provider"])

    # form json file
    json_str = {"dataProviderName":dataProvider_name, \
                "itemCount":item_count, \
                "sourceresource":{ \
                    "collection":{ \
                        "usage":usage_collection, \
                        "volume":volume_collection,\
                        "value":value_collection \
                    }, \
                    "contributor":{ \
                        "usage":usage_contributor, \
                        "volume":volume_contributor,\
                        "value":value_contributor \
                    }, \
                    "creator":{ \
                        "usage":usage_creator, \
                        "volume":volume_creator,\
                        "value":value_creator \
                    }, \
                    "date":{ \
                        "usage":usage_displayDate, \
                        "volume":volume_displayDate, \
                        "value":value_displayDate \
                    }, \
                    "description":{ \
                        "usage":usage_description, \
                        "volume":volume_description,\
                        "value":value_description \
                    }, \
                    "collection":{ \
                        "usage":usage_format, \
                        "volume":volume_format,\
                        "value":value_format \
                    }, \
                    "genre":{ \
                        "usage":usage_genre, \
                        "volume":volume_genre,\
                        "value":value_genre \
                    }, \
                    "identifier":{ \
                        "usage":usage_identifier, \
                        "volume":volume_identifier,\
                        "value":value_identifier \
                    }, \
                    "language":{ \
                        "usage":usage_language, \
                        "volume":volume_language,\
                        "value":value_language \
                    }, \
                    "place":{ \
                        "usage":usage_place, \
                        "volume":volume_place,\
                        "value":value_place \
                    }, \
                    "subject":{ \
                        "usage":usage_subject, \
                        "volume":volume_subject,\
                        "value":value_subject \
                    }, \
                    "title":{ \
                        "usage":usage_title, \
                        "volume":volume_title,\
                        "value":value_title \
                    }, \
                    "type":{ \
                        "usage":usage_type, \
                        "volume":volume_type,\
                        "value":value_type \
                    } \
                }, \
                "provider":{ \
                    "usage":usage_provider, \
                    "volume":volume_provider,\
                    "value":value_provider \
                }}

    # save the base dpla data to file
    base_dpla_data_file = open(base_dpla_filename, 'w')
    base_dpla_data_file.write(json.dumps(json_str))
    base_dpla_data_file.close()

    sys.exit(0)

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

# return 1 if the collection volume field>0,
# if filed=0 mean there is no usage of this field
def get_usage(collection_volume_field):
    if collection_volume_field > 0:
        return 1
    else:
        return 0

# setup argument parser
parser = argparse.ArgumentParser(description='please insert parameters')
parser.add_argument("-f", "--filename",
                    help="the location for storing the dpla statistics." +\
                    " If not set, these statistics are stored in: " + \
                    FILENAME_dpla_data_output)
parser.add_argument("-n", "--dataProvider",
                    help="Name of Data Provider")

# parse arguments received from the command line
args = parser.parse_args()

dpla_data_output_filename = FILENAME_dpla_data_output
#set the output filename
if args.filename:
   dpla_data_output_filename = args.filename

#call the profiler and let it do its magic
dataProvider(args.dataProvider, dpla_data_output_filename)
