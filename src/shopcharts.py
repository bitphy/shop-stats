#! /usr/bin/env python3
"""
    This script generates different charts for the shopstats results
    stored in a csv file
"""

import shopstats
import pandas as pd
import re
import datetime
import sys

def get_data_context_from_filename(filename):
    """ given the csv filename
        it gets the month and year if present, and 
        composes the context info for the charts

        >>> get_data_context_from_filename('shopstats_201906.csv')
        'June 2019'
        >>> get_data_context_from_filename('shopstats.csv')
        'test'¡
    """
    m = re.match('.*_(\d{4})(\d{2})\.csv', filename)
    if m:
        context = datetime.date(year=int(m.group(1)), month=int(m.group(2)), day=1).strftime('%B %Y')
    else:
        context = 'test'
    return context


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: %s «shopstats filename.csv»" % sys.argv[0])
        sys.exit(1)
    csv_filename = sys.argv[1]
    df = pd.read_csv(csv_filename)
    distinct_chart_filename = 'distinct.png'
    malformed_chart_filename = 'malformed.png'
    billing_chart_filename = 'billing.png'
    context = get_data_context_from_filename(csv_filename)
    shopstats.save_malformed_stats_chart(df, malformed_chart_filename, context)
    shopstats.save_sales_stats_chart(df, billing_chart_filename, context)
    shopstats.save_duplicated_stats_chart(df, distinct_chart_filename, context)
