#! /usr/bin/env python3
"""
    This script constructs a pandas DataFrame for the raw nodepoints
"""
from typing import Dict, Union, List
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import json
import logging
import datetime
import sys

from api_de_alberto import DateRanges, init_sqlalchemy_engine
from historic_algorithm_generator import historic_generation
from bitlog import init_logger

ENG = init_sqlalchemy_engine()

# Set up logging
logger = init_logger(__name__, __file__)

DATERANGES = [
    DateRanges.monthly
]

# File containing bitphy api connection params
api_params_filename = 'bitphyaccess.json'

# it contains the api parameters obtained from the api_params_filename
api_params = None

# Types of nodepoints are:
#   'raw': intended for raw nodepoints.
#   'sales': intended for sales nodepoints.
nodepoint_specs = [
        # { "name": "customers"                , "type": "raw"         , "equality_key": "originalId" , "column_suffix": "distinct" }    ,
        { "name": "product-categories"       , "type": "raw"         , "equality_key": "originalId" , "column_suffix": "distinct" }    ,
        { "name": "products"                 , "type": "raw"         , "equality_key": "originalId" , "column_suffix": "distinct" }    ,
        { "name": "sellers"                  , "type": "raw"         , "equality_key": "originalId" , "column_suffix": "distinct" }    ,
        { "name": "tickets"                  , "type": "raw"         , "equality_key": "originalId" , "column_suffix": "distinct" }    ,
        { "name": "customers/sales"          , "type": "aggregation" , "aggregation_key": "billing" , "subkey": "sales"                , "column_suffix": "billing" } ,
        { "name": "product-categories/sales" , "type": "aggregation" , "aggregation_key": "billing" , "subkey": "productCategorySales" , "column_suffix": "billing" } ,
        { "name": "products/sales"           , "type": "aggregation" , "aggregation_key": "billing" , "subkey": "productSales"         , "column_suffix": "billing" } ,
        { "name": "sales"                    , "type": "aggregation" , "aggregation_key": "billing" , "subkey": None                   , "column_suffix": "billing" } ,
        { "name": "sellers/sales"            , "type": "aggregation" , "aggregation_key": "billing" , "subkey": "sales"                , "column_suffix": "billing" } ,
        ]

# Functions to process different types of entries


def compute_raw_entries(nodepoint_spec, entries):
    """ Computes the counters of a given raw nodepoint entries
        returns counters as a tuple
        - number of items in the nodepoint
        - number of distinct items
        - number of malformed items. i.e. those not containing the identity key

        Note: the nodepoint_spec is not required for processing raw nodepoints.
        However it is included to uniform processing functions
    """
    nodepoint_equality_key = nodepoint_spec['equality_key']
    identity_values = [ entry[nodepoint_equality_key] for entry in entries if nodepoint_equality_key in entry ]
    counter = len(identity_values)
    distinct = len(set(identity_values))
    malformed = len(entries) - len(identity_values)
    return counter, distinct, malformed


def compute_aggregated_entries(nodepoint_spec, entries):
    """ Computes the counters of a given aggregated nodepoint entries
        returns counters as a tuple
        - number of items in the nodepoint
        - the aggregated value of the entries as defined by the nodepoint_specs
        - number of malformed items. i.e. those not containing the identity key
    """
    counter = 0
    aggregation = 0
    malformed = 0
    if nodepoint_spec['subkey']:
        # getting actual subentries
        subentries = []
        for entry in entries:
            if nodepoint_spec['subkey'] not in entry:
                logging.warning("Entry for node %s doesn't contin expected subkey %s: %s" % (nodepoint_spec['name'], nodepoint_spec['subkey'], entry))
                malformed += 1
                continue
            subentries += entry[nodepoint_spec['subkey']]
    else:
        subentries = entries

    # aggregating subentries
    for entry in subentries:
        if nodepoint_spec['aggregation_key'] not in entry:
            logging.warning("Entry for nodepoint %s doesn't contain aggregation key %s: %s" % (nodepoint_spec['name'], nodepoint_spec['aggregation_key'], entry))
            malformed += 1
            continue
        aggregation += entry[nodepoint_spec['aggregation_key']]
        counter += 1

    return counter, aggregation, malformed


entries_processors = {
        'raw': compute_raw_entries,
        'aggregation': compute_aggregated_entries,
        }


# Query string to define the parameters for the queries
# ATTENTION: some queries do not need all the params included in this query
querystring = None

# generate_shop_params(shop_id)
def generate_shop_params():
    """Dado un shop_id, genera un histórico de fechas,
    para hacer las peticiones necesarias en cada shop, de mes a mes"""
    """@historic_generation(shop_id=shop_id,
                         query_table='CustomerStats',
                         allowed_date_ranges=DATERANGES,
                         engine=ENG)
                         """
    # params= start_date, end_date, date_range
    def compose_querystring():
        """Compose a querystring for each shop_id, using dates generator"""
        query_string = [
                        {"dateStart": '2019-04-01', "dateEnd": '2019-04-30', "dateRange": "3"},
                        {"dateStart": '2019-05-01', "dateEnd": '2019-05-31', "dateRange": "3"},
                        {"dateStart": '2019-06-01', "dateEnd": '2019-06-30', "dateRange": "3"},
                        {"dateStart": '2019-07-01', "dateEnd": '2019-07-31', "dateRange": "3"},
                        {"dateStart": '2019-08-01', "dateEnd": '2019-08-31', "dateRange": "3"},
                        {"dateStart": '2019-09-01', "dateEnd": '2019-09-30', "dateRange": "3"}]
        return query_string

    return compose_querystring()


def get_querystring():
    """ returns the query params
        dateStart: first day of this month
        dateEnd: first day of next month
        dateRange: 3 (monthly date only)
    """
    global querystring
    if not querystring:
        today = datetime.date.today()
        first_day_this_month = today.replace(day=1)
        first_day_next_month = first_day_this_month.replace(month=(today.month % 12 + 1))
        querystring = {"dateStart": first_day_this_month, "dateEnd":first_day_next_month, "dateRange":"3"}
    return querystring

def get_current_month_as_title():
    """ returns current month and year
        e.g. June 2019
    """
    return get_querystring()['dateStart'].strftime('%B %Y')

# filenames for the different outputs
filename_templates = {
        'csv': 'shopstats_%s.csv',
        'malformed_chart': 'malformed_%s.png',
        'billing_chart': 'billing_%s.png',
        'dup_chart': 'distinct_%s.png',
        }

def get_filename(base):
    """ returns the required filename composed with the month and year """
    return filename_templates[base] % get_querystring()['dateStart'].strftime('%Y%m')


# requests modules
def response_is_ok(response):
    """ returns True when
        - response status code is ok and
        - contents type is json
    """
    if not (response.status_code < 400):
        logging.warning("\tResponse code is not ok: %s" % response.headers)
        return False
    if response.headers.get('content-type') != 'application/json; charset=utf-8':
        logging.warning("\tResponse content type is not ok: %s" % response.headers)
        return False
    return True


def get_api_params(filename=api_params_filename):
    """ Loads connection params """
    global api_params
    if not api_params:
        with open(filename) as f:
            api_params = json.load(f)
    return api_params


def get_shops():
    """ Returns the list of available shops as a tuple
        (chain_id, shop_id, name)
    """
    api_params = get_api_params()
    url_base = api_params['url_base']
    headers = api_params['headers']
    url = '%s/user/accessible-resources' % url_base
    logging.info("get_shops() loading shops from node %s" % url)
    response = requests.request("GET", url, headers=headers)
    if not response_is_ok(response):
        return []
    chains = json.loads(response.text, encoding = 'utf-8')
    shops = []
    for chain in chains:
        if chain['id'] != 'pbnhlongag6ata':
            continue
        if 'id' not in chain:
            logger.warning('chain_id not found in accessible-resources %s' , chain)
            continue
        chain_id = chain['id']
        for shop in chain.get('shops'):
            if shop['id'] != '91zdnpvz08x68q':
                continue
            shops.append((chain_id, shop['id'], shop['name']))
        # shops += [ (chain_id, shop['id'], shop['name']) for shop in chain.get('shops') ]
    logger.info("\tshops: %s" % shops)
    return shops


def get_nodepoint_entries(chain_id, shop_id, nodepoint, filters):
    """ given a chain, a shop and a nodepoint
        it calls the API to get the corresponding entries.
        It returns the tuple
        - result: the result of the call: 'ok', 'error'
        - the contents as a list. Empty on error
    """
    nodepoint_url = '/chains/%s/shops/%s/%s' % (chain_id, shop_id, nodepoint)
    api_params = get_api_params()
    url_base = api_params['url_base']
    headers = api_params['headers']
    url = '%s%s' % (url_base, nodepoint_url)
    logger.info('Requesting: %s' % url)
    response = requests.request("GET", url, headers=headers, params=filters)
    if not response_is_ok(response):
        return ('error', [])
    resultat = json.loads(response.text, encoding = 'utf-8')
    # logger.info('\tresultat: %s' % resultat)
    return ('ok', resultat)


def get_nodepoint_counters(chain_id, shop_id, nodepoint_spec, current_param):
    """ Computes the counters of a given nodepoint and returns them as a tuple """
    nodepoint_name = nodepoint_spec['name']

    (result, entries) = get_nodepoint_entries(chain_id, shop_id, nodepoint_name, current_param)
    if result == 'error':
        logger.warning("get_nodepoint_counters(chain_id: %s, shop_id: %s, nodepoint: %s) An error was found with result '%s'" % (chain_id, shop_id, nodepoint_spec, entries))
        return (np.nan, np.nan, np.nan)

    return entries_processors[nodepoint_spec['type']](nodepoint_spec, entries)


def compose_nodepoint_column(nodepoint_spec):
    """ Composes a list of column names for this nodepoint
        It generates three columns 'count', column_suffix, and 'malformed', prefixed
        by the nodepoint name

        >>> compose_nodepoint_column( { "name": "sellers", "column_suffix": "distinct" })
        ['sellers_count', 'sellers_distinct', 'sellers_malformed']
    """
    nodepoint_name = nodepoint_spec['name']
    column_suffix = nodepoint_spec['column_suffix']
    return [ '%s_%s' % (nodepoint_name, column) for column in ('count', column_suffix, 'malformed') ]


def compose_initial_rows_data() -> List[Dict]:
    """Compose a list of dictionaries with the detailed data corresponding
    to each shop ('date', 'chain_id', 'shop_id', 'shop_name').
    It will be used to build a base for the final data frame of data.
    """
    initial_data: List[Dict] = []
    for chain_id, shop_id, shop_name in get_shops():
        dates_ranges = generate_shop_params()
        for dates in dates_ranges:
            row = {'date': dates['dateStart'],
                   'chain_id': chain_id,
                   'shop_id': shop_id,
                   'shop_name': shop_name}
            initial_data.append(row)
    return initial_data


def generate_dataframe(nodepoints_specs) -> pd.DataFrame:
    """ given a list with nodepoints specs
        it generates a dataframe containing
        a row for each shop of the chain and
        for each raw nodepoint: three columns (count, distinct, malformed)
    """

    def build_dataframe(nodepoint_specs) -> pd.DataFrame:
        """Build the columns of the dataframe,
        returns the empty dataframe, except for the initial columns
        ['date', 'chain_id', 'shop_id', 'shop_name']
        that are filled in with their corresponding data,
        generating the necessary rows in the dataframe."""
        initial_data = compose_initial_rows_data()
        columns = ['date', 'chain_id', 'shop_id', 'shop_name']
        for nodepoint_spec in nodepoint_specs:
            columns += compose_nodepoint_column(nodepoint_spec)
        df_initial = pd.DataFrame(initial_data, columns=columns, dtype=np.int64)

        return df_initial

    def populate_shops(nodepoint_specs) -> pd.DataFrame:
        """For each shop, get the data and add to the dataframe of all stores
        """
        shops = get_shops()
        df_shops = build_dataframe(nodepoints_specs)
        for chain_id, shop_id, _ in shops:
            df_shop = df_shops.copy()
            df_shop = populate_counters_by_shop(df_shop, nodepoint_specs, chain_id, shop_id)
            df_shops = df_shops.combine_first(df_shop)

        return df_shops

    def populate_counters_by_shop(df, nodepoint_specs,
                                  chain_id, shop_id) -> pd.DataFrame:
        """Fill in the month-to-month dataframe of a single shop,
        and finally combine it with the shop's base dataframe
        """
        df_shop = df.copy()
        df_month = df.copy()
        # TODO aquí habria de pasar un shop_id como parámetro cuando se use historic generator
        date_ranges = generate_shop_params()
        for param in date_ranges:
            for nodepoint_spec in nodepoint_specs:
                # df_month will be filled in every lap.
                df_month = populate_counters_by_month(df_month, chain_id, shop_id, nodepoint_spec, param)
            df_shop = df_shop.combine_first(df_month)
        return df_shop

    def populate_counters_by_month(df, chain_id, shop_id,
                                   nodepoint_spec, param) -> pd.DataFrame:
        """Get columns with entries for an nodepoint_spec and
        enter them in the given dataframe."""

        counters = get_nodepoint_counters(chain_id, shop_id, nodepoint_spec, param)
        # add counters of current nodepoint
        df_mask = (df['shop_id'] == shop_id) & (df['date'] == param['dateStart']),\
            compose_nodepoint_column(nodepoint_spec)
        df.loc[df_mask] = counters
        return df

    df_shops = populate_shops(nodepoints_specs)

    return df_shops


def sort_columns(df):
    """ returns the dataframe with the columns sorted as:
        - chain_id, shop_id, shop_name
        - *_billing
        - rest
        - *_malformed
    """
    identity_columns = [ 'chain_id', 'shop_id', 'shop_name' ]
    billing_columns = [ column for column in df.columns if column.endswith('_billing') ]
    malformed_columns = [ column for column in df.columns if column.endswith('_malformed') ]
    rest_columns = [ column for column in df.columns if column not in identity_columns + billing_columns + malformed_columns ]
    return df[identity_columns + billing_columns + rest_columns + malformed_columns]


def save_malformed_stats_chart(shopstats, filename, date_title = get_current_month_as_title()):
    """ given the shop stats DataFrame,
        it saves a png with the malformed stats

        :param shopstats: DataFrame containing the data generated by shopstats
        :param filename:  the name of the file where the generated chart will be saved
        :param date_title:  description of the period included in the chart

        What does it shows:
        - number of malformed entries for each nodepoint
        - non zero cells are highlighted


        TODO: there's some code redundancy with other functions that
        save different charts. Consider some refactoring
    """
    def prepare_data(shopstats):
        columns = [ column[:-len('_malformed')]
                              for column in shopstats 
                              if column.endswith('_malformed')]

        values = pd.DataFrame(columns=['index'] + columns)
        values['index'] = shopstats.apply(lambda row: "%s/%s/%s" % (row.chain_id, row.shop_id, row.date), axis=1)
        values[columns] = shopstats[
                [ '%s_malformed'%nodepoint
                  for nodepoint in columns ]]
        values = values.set_index(['index'])
        values = values.astype('int')

        # fake data for testing
        # comment the following lines out to get some wrong billings
        #values.loc['DEMO/zqrvxircdzczvg', 'products'] += 1
        return values

    def compose_chart(values, title):
        sns.set()
        f, ax = plt.subplots(figsize=(15, 6))
        sns.heatmap(values,
                annot=True, fmt='d',        # show values
                linewidths=.5,
                cmap='bwr',
                vmax=1,
                cbar=False                  # hide scale bar
                )
        ax.xaxis.set_ticks_position('top')  # x labels to top
        plt.xlabel('')
        plt.ylabel('')
        plt.title(title)
        plt.xticks(rotation=25)
        f.tight_layout()

    values = prepare_data(shopstats)
    title = 'malformed entries - %s' % date_title
    compose_chart(values, title)
    plt.savefig(filename)


def save_sales_stats_chart(shopstats: pd.DataFrame,
                           filename: str,
                           date_title:str = get_current_month_as_title()):
    """ given the shop stats DataFrame,
        it saves a png with the sales stats.

        :param shopstats: DataFrame containing the data generated by shopstats
        :param filename:  the name of the file where the generated chart will be saved
        :param date_title:  description of the period included in the chart

        What does it shows:
        - sales: the billing of each shop
        - products/sales and sellers/sales: the billing for products and
          sellers. When different to the shop sales they're highlighted
        - product-categories/sales and customers/sales: the billing for these
          nodepoints. When they sum over the shop sales they're highlighted
    """

    def prepare_data_to_display(shopstats):
        """ data to be displayed in the cells of the chart """
        columns = [ column[:-len('_billing')]
                    for column in shopstats
                    if column.endswith('_billing')]
        values = pd.DataFrame(columns=['index'] + columns)
        values['index'] = shopstats.apply(lambda row: "%s/%s/%s" % (row.chain_id, row.shop_id, row.date), axis=1)
        values[columns] = shopstats[
                ['%s_billing' % nodepoint
                 for nodepoint in columns]]
        values = values.set_index(['index'])

        # Columns are sorted for readability
        values = values[['sales', 'products/sales', 'sellers/sales', 'customers/sales', 'product-categories/sales']]

        # fake data for testing
        # comment the following lines out to get some wrong billings
        #values.loc['DEMO/zqrvxircdzczvg', 'products/sales'] -= 1
        #values.loc['DEMO/rnr49unsl599e9', 'sellers/sales'] -= 5
        #values.loc['DEMO/ohokznuz7yy9jh', 'product-categories/sales'] = values.loc['DEMO/ohokznuz7yy9jh', 'sales'] + 100
        #values.loc['DEMO/nnvk70vf4jq5cd', 'customers/sales'] = values.loc['DEMO/nnvk70vf4jq5cd', 'sales'] + 200
        return values

    def prepare_data_to_highlight(values):
        """ data to be used by the heatmap to highlight potentially
            problematic cells """
        base = values.copy()
        base[["sales"]] = 0.0
        base["sellers/sales"] = np.logical_not(np.isclose(values['sellers/sales'], values['sales'])).astype('float')
        base["products/sales"] = np.logical_not(np.isclose(values['products/sales'], values['sales'])).astype('float')
        base["product-categories/sales"] = (values['product-categories/sales'] > values['sales'] + 0.000001).astype('float')
        base["customers/sales"] = (values['customers/sales'] > values['sales'] + 0.000001).astype('float')
        return base

    def compose_chart(base, values, title):
        sns.set()
        f, ax = plt.subplots(figsize=(15, 6))
        sns.heatmap(base,
                annot=values,
                fmt='.2f',        # show values
                linewidths=.5,
                cmap='bwr',
                cbar=False                  # hide scale bar
                )
        ax.xaxis.set_ticks_position('top')  # x labels to top
        plt.xlabel('')
        plt.ylabel('')
        plt.title(title)
        plt.xticks(rotation=25)
        f.tight_layout()

    values = prepare_data_to_display(shopstats)
    base = prepare_data_to_highlight(values)
    title = 'billing entries - %s' % date_title
    compose_chart(base, values, title)
    plt.savefig(filename)


def save_duplicated_stats_chart(shopstats: pd.DataFrame,
                           filename: str,
                           date_title:str = get_current_month_as_title()):
    """ given the shop stats DataFrame,
        it saves a png with the duplicated stats.

        :param shopstats: DataFrame containing the data generated by shopstats
        :param filename:  the name of the file where the generated chart will be saved
        :param date_title:  description of the period included in the chart

        What does it shows:
        - a column for each raw nodepont
        - for each cell, it shows a pair (counter/unique)
        - when counter > unique the cell is highlighted
    """

    def prepare_data(shopstats):
        # fake data for testing
        # comment the following lines out to get some dups
        #shopstats.loc[0, 'products_count'] += 1
        #shopstats.loc[1, 'customers_count'] += 10

        # raw nodepoints have a column ending with _distinct
        raw_nodepoints = [ column[:-len('_distinct')] for column in shopstats.columns if column.endswith('_distinct')]

        # prepare data to be shown
        #   values: contents to be shown in the cells
        #   base:   contents to be considered to decide highlighting the cell
        values = pd.DataFrame(columns=['index'] + raw_nodepoints)
        values['index'] = shopstats.apply(lambda row: "%s/%s/%s" % (row.chain_id, row.shop_id, row.date), axis=1)
        base = values.copy()
        for nodepoint in raw_nodepoints:
            values[nodepoint] = shopstats['%s_distinct' % nodepoint].astype('int').astype("str") + '/' + shopstats['%s_count' % nodepoint].astype('int').astype("str")
            base[nodepoint] = (shopstats['%s_count' % nodepoint] != shopstats['%s_distinct' % nodepoint]).astype('float')
        values = values.set_index(['index'])
        base = base.set_index(['index'])
        return base, values

    def compose_chart(base, values, title):
        sns.set()
        f, ax = plt.subplots(figsize=(15, 6))
        sns.heatmap(base,
                annot=values,
                fmt='s',                    # string values
                linewidths=.5,
                cmap='bwr',
                cbar=False                  # hide scale bar
                )
        ax.xaxis.set_ticks_position('top')  # x labels to top
        plt.xlabel('')
        plt.ylabel('')
        plt.title(title)
        plt.xticks(rotation=25)
        f.tight_layout()

    base, values = prepare_data(shopstats)
    title = 'distinct entries - %s' % date_title
    compose_chart(base, values, title)
    plt.savefig(filename)


if __name__ == "__main__":
    logging.basicConfig(filename='%s.log'%sys.argv[0],level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    logging.info('\n'*3)
    # obtain results
    df = generate_dataframe(nodepoint_specs)
    # Store results
    df = sort_columns(df)
    df.to_csv(get_filename('csv'))
    print("Output saved at %s" % get_filename('csv'))
    save_malformed_stats_chart(df, get_filename('malformed_chart'))
    print("Malformed stats chart saved at %s" % get_filename('malformed_chart'))
    save_sales_stats_chart(df, get_filename('billing_chart'))
    print("Sales stats chart saved at %s" % get_filename('billing_chart'))

    save_duplicated_stats_chart(df, get_filename('dup_chart'))
    print("Duplicated stats chart saved at %s" % get_filename('dup_chart'))



