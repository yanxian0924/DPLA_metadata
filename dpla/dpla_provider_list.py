__author__ = 'Yanxian Liu'
"""
Retrieves unique values for provider attribute and the number of unique values
sample command: python3 dpla_provider_list.py [-f <filename>]

"""
import sys
import json
import dpla_utils
import profiler_config as config
import argparse

from pprint import pprint

DPLA_API_QUERY_facets = "facets"
DPLA_PROP_providerName = "provider.name"
DPLA_API_PROP_terms = "terms"
DPLA_API_PROP_term = "term"
DPLA_API_PROP_count = "count"

FILENAME_dpla_data_output = "dpla-provider-list.json"

def getAllProviders(base_dpla_filename):
    provider_dict = {}

    query_provider_facets = {DPLA_API_QUERY_facets: DPLA_PROP_providerName}

    response_provider_facets = dpla_utils.dpla_fetch_facets_remote( \
                                api_key=config.API_KEY, **query_provider_facets)
    
    for provider in response_provider_facets[DPLA_PROP_providerName][DPLA_API_PROP_terms]:
        provider_dict[provider[DPLA_API_PROP_term]] = provider[DPLA_API_PROP_count]

    # save the base dpla data to file
    base_dpla_data_file = open(base_dpla_filename, 'w')
    base_dpla_data_file.write(json.dumps(provider_dict))
    base_dpla_data_file.close()

    sys.exit(0)

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
getAllProviders(dpla_data_output_filename)
