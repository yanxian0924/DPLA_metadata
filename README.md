# DPLA metadata
This project is for INF 385T at the [iSchool](https://www.ischool.utexas.edu/) at the University of Texas at Austin.

## Project Goals
This project aims to use metadata available from the Digital Public Library of America to create profiles of the holdings of each individual institution that contributes to the DPLA.

## Data Schema
We use a subset of the metadata available in the DPLA's MAP (Metadata Application Profile).  Specifically, we use a number of properties from the source.Resource namespace (?).

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

## Interface and Syntax
The program is written in Python and accessed through a command line interface.  The basic syntax, when finished, should be something like this:

[dataProviderName] [metadata fields to query] [things to count]

## Interface completed:
 * Retrieves sourceResource data from the DPLA according to a specific data provider as input

 python3 dpla_dataProvider.py [-n <dataProvider name>] [-f <filename>]

 Default output filename is dpla-dataProvider-data.json.

Example:
python3 dpla_dataProvider.py -n "Lyndon Baines Johnson Library"

Requirements:
Install python3 and requests package (http://docs.python-requests.org/en/master/user/install/)
