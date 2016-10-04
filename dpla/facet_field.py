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

# a list to store unique values
unique_values = []

# a dict to store unique values with count
unique_count = {}

# flag to determine if all items have one and only one value
all_row_value_flag = 'true'

# count no of items missing value for the field
missing_count = 0

# iterate row by row
for row in content:
    row_values = []
    if args.fields not in row:
        # change flag to false as at least one item does not contain the queried field
        missing_count = missing_count + 1
        all_row_value_flag = 'false'
    else:
        # get values of row in row_values
        row_values = row[args.fields]
        # some fields return a list whereas others return just one value.
        # check to see whether output is a list or no
        if type(row_values) is not list:
            # check to see if value is already in unique_count
            if row_values not in unique_count.keys():
                unique_count[row_values] = 1
                # making a list for uniques without the count
                unique_values.append(row_values)
            else:
                unique_count[row_values] = int(unique_count.get(row_values)) + 1
        else:
            if len(row_values) > 1:
                # change flag to false as at least one item has more than one value. 
                all_row_value_flag = 'false'
            # since this is a list, iterate through values
            for value in row_values:
                # check to see if value is already in unique_count
                if value not in unique_count.keys():
                    unique_count[value] = 1
                    # making a list for uniques without the count
                    unique_values.append(value)
                else:
                    unique_count[value] = unique_count.get(value) + 1

sorted_unique_count = sorted(unique_count.items(), key = operator.itemgetter(1), reverse = True)                    
# print(unique_values)
print(sorted_unique_count)

# Number of items missing value for the field
print("Number of items missing value for the field "+ str(missing_count))

# check flag, false means all items do not have one and only one value
if all_row_value_flag == 'false':
    print("All items do not have one and only one value in the " + args.fields + " field")
else:
    print("All items have one and only one value in the " + args.fields + " field")
    

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
     json.dump(sorted_unique_count, outfile)
     outfile.close()