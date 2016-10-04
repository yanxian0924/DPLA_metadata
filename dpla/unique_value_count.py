import argparse
import dpla_utils
import json
import operator

"""
An example to run the file in command line
>>> python3 facet_field.py -r -k 8034648d3e9920c1357ccb6a9a8f9756 -f sourceResource.language.name -i 9bb1879d16884108685783ce9053d276 -n uniqueOutput
"""

# command line interface that saves arguements to args
parser = argparse.ArgumentParser(description = "please insert parameters")
# use local or Remote, put -LoR for Remote. if not set, use local
parser.add_argument("-r", "--remote", help = "access local or remote server,\
                    remote by default", action = "store_true")
# api_key for access DPLA server
# api_key="8034648d3e9920c1357ccb6a9a8f9756"
parser.add_argument("-k", "--api_key", help = "api_key")
# field name to find unique values
parser.add_argument("-f", "--fields", help = "field to facet")
# DPLA collection id which is to be queried
parser.add_argument("-i", "--id",
                    help = "specific the id of the collection you want to profile, if not set, it goes through every collection in the record",nargs = "*")
# name to be used to save output file with
parser.add_argument("-n", "--name", help = "output file name")
args = parser.parse_args()

# create query used to derive content
query = {}
if args.api_key:
    query["api_key"] = args.api_key

# querying the DPLA through dpla_util methods
content = (dpla_utils.dpla_get_uniques(remote=args.remote, fields = args.fields, collection_id = args.id, **query))

uniques = dpla_utils.unique_value_count(args.fields, content)

print(uniques)

# Number of items missing value for the field
#print("Number of items missing value for the field "+ str(missing_count))

# check flag, false means all items do not have one and only one value
"""
if all_row_value_flag == 'false':
    print("All items do not have one and only one value in the " + args.fields + " field")
else:
    print("All items have one and only one value in the " + args.fields + " field")
    """

# naming algorithm
# if filename is not specified then the collection id is used for the name of the output file, in case collection id is also not specified then api key is used                            
if args.name is not None:
     title = args.name + ".json"
elif args.id is not None:
     title = args.id + ".json"
else:
     title = args.api_key + ".json"
 
#saving the output to JSON file
with open(title, "w") as outfile:
     json.dump(uniques, outfile)
     outfile.close()
