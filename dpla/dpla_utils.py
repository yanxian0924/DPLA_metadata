from __future__ import print_function

"""
:Author: Madhura Parikh (madhuraparikh@gmail.com)
            Zhang(zack@zackz.net)

This module contains basic utility functions for querying dp.la.
Also supports interactions with a mongodb database
"""

import dpla_utils_config as config
import logging

import operator
import requests
import pymongo
import pandas
import matplotlib.pyplot as plt
# elasticsearch import commented by unmil on 2/21/2016. This will cause the local functions to malfunction at best.
#from elasticsearch import Elasticsearch


# define some common variables that can be used globally
collections_url = config.COLLECTIONS_URL
items_url = config.ITEMS_URL


def send_request(url, payload):
    """
    This function sends a HTTP get request to the given url and returns the corresponding
    response parsed as json.

    Parameters
    ----------
    url : str
        The url of the remote server.
    payload : dict
        The dictionary representing the params of the HTTP request

    Returns
    -------
    json parse of `Requests.response` object.

    Examples
    --------
    >>> payload = { 'api_key' : 000000 }
    >>> dpla_json = send_request(collections_url, payload)
    """
    try:
        response = requests.get(url, params=payload)
        # print(payload)
        return response.json()

    except requests.exceptions.RequestException:
        raise Exception("HTTP request failed.")


def dpla_fetch(remote=True, **kwargs):
    """
    Method that fetches dpla data from local or remote database.

    Parameters
    ----------
    remote : boolean, optional
        Specifies whether the data should be fetched from remote or local database. 
        When True, fetches from remote dpla server, when False, fetches from elastic search.
        Defaults to True.

    Returns
    -------
    JSON object 

    Examples
    --------
    For documentation of each fetch, see the `dpla_fetch_local` and `dpla_fetch_remote` methods.
    Each argument expected by those functions must be specified as keyword.

    >>> json_result = dpla_fetch(remote=True, api_key='00000', count=1000)  # fetches remote data
    >>> json_result = dpla_fetch_local(remote=False, collection_id='0', count=1000) # fetches elastic search data
    """
    if remote:
        api_key = kwargs.get('api_key')

        try:
            assert api_key is not None

            del kwargs['api_key']

            return dpla_fetch_remote(api_key, **kwargs)
        except AssertionError:
            raise Exception('For remote fetch api_key and count params must be specified')
    else:
        # collection_id = kwargs.get('collection_id')
        api_key = kwargs.get('api_key')

        try:
            # assert collection_id is not None
            assert api_key is not None
            del kwargs['api_key']

            # del kwargs['collection_id']

            return dpla_fetch_local(**kwargs)
        except AssertionError:
            raise Exception('For local fetch collection_id and count params must be specified')


def dpla_fetch_remote(api_key, search_type='items', **kwargs):
    """
    Fetches the specified number of resources with simple search by default, and
    supports more selective queries by optional arguments.

    Parameters
    ----------
    api_key : str
        The api key generated from dp.la.
    count : int
        The number of resources to be fetched.
    search_type : str, optional
        the type of resource to fetch. Should be either 'items' or 'collections'.
        Defaults to 'items'.

    Returns
    -------
    dpla_results : dict
        The collections from dpla in JSON format.

    Examples
    --------
    >>> api_key = '000000'

    >>> # fetches the first 1200 items
    >>> items = dpla_fetch(api_key, 1200)

    >>> # fetches all items that mention kitten somewhere
    >>> items = dpla_fetch(api_key, q='kitten*')

    >>> # for nested fields (that have periods in them) do this
    >>> conditions = { 'sourceResource.collection.title' : 'Smith' }
    >>> # this will return all items from the colletion titled Smith
    >>> items = dpla_fetch(api_key, **conditions)
    """
    if 'count' in kwargs:
        count = kwargs.get('count')
        del kwargs['count']
    else:
        count = 0
    # first of all set the page_size
    page_size = 500

    # now construct the payload dict
    payload = dict()
    payload['api_key'] = api_key
    for key in kwargs:
        payload[key] = kwargs[key]

    # fetch the query results
    if search_type == 'collections':
        url = collections_url
    else:
        url = items_url
    json_dics = []
    # fetch the count of result
    if count == 0:
        response = send_request(url, payload)
        count = response.get('count')
        # print("Total result: %s" % count)
    #print("count: " + count)
    #print("page_size: " + page_size)
    final_page_size = count % page_size
    num_pages = int(count / page_size)
    page_count = 0
    if 'page' in payload:
        if 'page_size' in payload:
            response = send_request(url, payload)
            json_dics.append(response)
    else:
        for i in range(1, num_pages + 1, 1):
            payload['page_size'] = 500
            payload['page'] = i
            response = send_request(url, payload)
            json_dics.append(response)
            page_count = i
        if final_page_size:
            # print( "ENTERED CONDITION FOR FINAL PAGE SIZE IN DPLA UTILS\n")
            payload['page'] = page_count + 1
            payload['page_size'] = 500
            response = send_request(url, payload)
            json_dics.append(response)
    # combine all the results
    dpla_results = []
    for dic in json_dics:
        docs = dic.get('docs', [])
        for doc in docs:
            dpla_results.append(doc)
    return dpla_results


def dpla_fetch_facets_remote(api_key, search_type='items', **kwargs):
    """
    Fetches the facets from the DPLA server

    Parameters
    ----------
    api_key : str
        The api key generated from dp.la.
    count : int
        The number of resources to be fetched.
    search_type : str, optional
        the type of resource to fetch. Should be either 'items' or 'collections'.
        Defaults to 'items'.

    Returns
    -------
    dpla_facets : dict
        The facets from dpla in JSON format.

    Examples
    --------
    >>> api_key = '000000'

    >>> # fetches the first 1200 items
    >>> items = dpla_fetch(api_key, 1200)

    >>> # fetches all items that mention kitten somewhere
    >>> items = dpla_fetch(api_key, q='kitten*')

    >>> # for nested fields (that have periods in them) do this
    >>> conditions = { 'sourceResource.collection.title' : 'Smith' }
    >>> # this will return all items from the colletion titled Smith
    >>> items = dpla_fetch(api_key, **conditions)
    """
    # now construct the payload dict
    payload = dict()
    payload['api_key'] = api_key
    for key in kwargs:
        payload[key] = kwargs[key]

    # fetch the query results
    if search_type == 'collections':
        url = collections_url
    else:
        url = items_url
    json_dics = []
    # fetch the  results
    response = send_request(url, payload)
    dpla_results = response.get('facets', [])
    return dpla_results


def dpla_fetch_local(**kwargs):
    if 'count' in kwargs:
        count = kwargs.get('count')
        del kwargs['count']
    else:
        count = 0

    start_index = 0

    payload = dict()
    for key in kwargs:
        payload[key] = kwargs[key]
    if 'page' in payload:
        if 'page_size' in payload:
            count = payload['page_size']
            start_index = payload['page_size'] * (payload['page'] - 1)
            del payload['page']
            del payload['page_size']

    if 'fields' in payload:
        return_fields = payload['fields']
        del payload['fields']
    else:
        return_fields = "*"
    if 'sort_by' in payload:
        del payload['sort_by']
    return_fields = return_fields.split(',')
    # initate a client
    logging.basicConfig()
    es = Elasticsearch([config.ES_URL])
    # compose a query
    if count == 0:
        query = {
            "_source": return_fields,
            "query": {
                "match": payload
            },

        }
    else:
        query = {
            "_source": return_fields,
            "query": {
                "match": payload
            },
            'from': start_index,
            'size': count
        }
    response = es.search(index=config.ES_INDEX, body=query)

    docs = []
    hits = response['hits']['hits']
    # parse the fileds
    for hit in hits:
        docs.append(hit['_source'])
    # convert to json format
    return docs


def connect_to_mongodb():
    """ Returns connection to a running mongodb server """
    try:
        conn = pymongo.MongoClient()
        # print("Connected successfully")
        return conn
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB", e)


def draw_bar_plot(values, max_to_plot=50, save_file=None, x_label='X axis', y_label='Y axis', x_dim=10, y_dim=10, sorted=True, xkcd=True):
    """
    Plots a bar graph given a dictionary of key-value pairs.

    Parameters
    ----------
    values : dict
        dict of the form x -> y
    max_to_plot : int
        the maximum values to plot on the bar plot,
        defaults to 50
    save_file : str, optional
        the name of the file where you want to store the bar plot. Must have extension .png
        If not specified, no file will be saved. If not the absoulte path, will be saved
        relative to current working directory.
    x_label : str, optional
        label for the x-axis
    y_label : str, optional
        label for the y axis
    x_dim : int, optional
        width of the bounding box, defaults to 10 units
    y_dim : int, optional
        height of the bounding box, defaults to 10 units
    sorted : bool, optional
        sort the columns in increasing order of counts.
        defaults to True
    xkcd : bool, optional
        plot the graph in xkcd style.
        defaults to true

    Returns
    -------
    None

    Examples
    --------

    >>> values = {'a' : 10, 'b' : 5, 'c' : 12}
    >>> draw_bar_plot(values, save_file='example.png')
    """

    if xkcd:
        plt.xkcd()
    count_series = pandas.Series(values)
    if sorted:
        count_series.sort(axis=1)
    if count_series.size > max_to_plot:
        count_series = count_series[: max_to_plot]
    count_series.plot(kind='bar', figsize=(x_dim, y_dim), ascending=False)
    plt.ylabel(y_label)
    plt.xlabel(x_label)
    if save_file:
        plt.savefig(save_file)
    plt.show()


def dpla_get_count(remote=True, **kwargs):
    if remote:
        api_key = kwargs.get('api_key')
        try:
            assert api_key is not None
            del kwargs['api_key']
            return dpla_get_count_remote(api_key, **kwargs)
        except AssertionError:
            raise Exception('For remote fetch api_key params must be specified')
    else:
        try:
            api_key = kwargs.get('api_key')
            assert api_key is not None
            del kwargs['api_key']

            return dpla_get_count_local(**kwargs)
        except AssertionError:
            raise Exception('For local fetch condition must be specified')


def dpla_get_count_remote(api_key, **kwargs):
    payload = dict()
    payload['api_key'] = api_key
    for key in kwargs:
        payload[key] = kwargs[key]
    response = send_request(items_url, payload)
    count = response.get('count')
    return count


def dpla_get_count_local(**kwargs):
    payload = dict()
    for key in kwargs:
        payload[key] = kwargs[key]
        # in case these argument are parsed in by mistake

    if 'page' in payload:
        del payload['page']
    if 'page_size' in payload:
        del payload['page_size']
    if 'fields' in payload:
        return_fields = payload['fields']
        del payload['fields']
    else:
        return_fields = "*"
    if 'sort_by' in payload:
        del payload['sort_by']
    # initiate a client
    logging.basicConfig()
    es = Elasticsearch([config.ES_URL])
    # compose a query
    query = {
        "_source": return_fields,
        "query": {
            "match": payload
        },
    }
    response = es.search(index=config.ES_INDEX, body=query)
    count = response['hits']['total']
    # parse the fileds
    return count


def dpla_get_collection_info(remote, collection_id, **kwargs):
    kwargs['sourceResource.collection.id'] = collection_id
    kwargs['count'] = 1
    docs = dpla_fetch(remote, **kwargs)
    # condition.basicConfig()
    # es = Elasticsearch([config.ES_URL])
    # query = {
    # "_source": "sourceResource.collection",
    # "query": {
    # "match": {"sourceResource.collection.id": collection_id}
    #     },
    #     "size":config.DU_Number_Sample
    # }
    # response = es.search(index=config.ES_INDEX, body=query)
    #
    # docs = response['hits']['hits']
    flag = False
    for doc in docs:
        collections = doc['sourceResource']['collection']
        if isinstance(collections, dict):
            if collections['id'] == collection_id:
                sample_collection = collections
            else:
                print("the item belong to this collection doesnt have the info fo this collection")
        elif isinstance(collections, list):
            for collection in collections:
                # print(collection)
                if collection['id'] == collection_id:
                    sample_collection = collection
                    flag = True
                    break
        if flag:
            break
    return sample_collection

"""
    Description for uniques method of dpla
    dpla_get_uniques(remote, fields, collection_id, **kwargs)

    Fetches the DPLA docs output

    Parameters
    ----------
    remote : boolean, optional
        Specifies whether the data should be fetched from remote or local database. When True, fetches from remote dpla server, when False, fetches from elastic search. Defaults to True.
    fields : str
        the field whose unique values are required.
    collection_id : int
        Fetch item details only for this particular collection id.
    
    Returns
    -------
    response_uniques : dict
        The collections from dpla in JSON format.

    Examples
    --------

    >>> # fetches the unique values and count for the rights field
    >>> items = dpla_get_uniques(remote=args.remote, fields = sourceResource.rights, collection_id = 9bb1879d16884108685783ce9053d276, **query)
"""

def dpla_get_uniques(remote, fields, collection_id, **kwargs):
    kwargs['sourceResource.collection.id'] = collection_id
    kwargs['fields'] = fields
    #fetch the query result
    response_uniques = dpla_fetch(remote, **kwargs)
    return response_uniques

def get_unique_value_count(field, dpla_result):
    """
    Retrieves the unique values and the count for each value for the specified
    field in the dpla_result

    Parameters
    -----------
    field : 
    dpla_result :

    Returns
    -------
    A dictionary consisting of an array of values and count of 
    occurrences for each value

    Examples:
    ---------
    """

    # a list to store unique values
    unique_values = []
    
    # a dict to store unique values with count
    unique_count = {}
    
    # flag to determine if all items have one and only one value
    all_row_value_flag = 'true'
    
    # count no of items missing value for the field
    missing_count = 0
    
    # iterate row by row
    for row in dpla_result:
        row_values = []
        if field not in row:
            # change flag to false as at least one item does not contain the queried field
            missing_count = missing_count + 1
            all_row_value_flag = 'false'
        else:
            # get values of row in row_values
            row_values = row[field]
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

    return sorted_unique_count

