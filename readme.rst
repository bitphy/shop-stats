##########################################
Reporting of shop stats for the Bitphy API
##########################################

This project consist of some scripts that extract information
from the BitPhy API and generates some charts to check for consistency.

Main use
========

Create a file named bitphyaccess.json in src/ containing:

::

    {
        "url_base":  "http://api.dev.bitphy.es/v2.0",
        "headers": {
            "Authorization": "Bearer «place here your access token»",
            "Cache-Control": "no-cache"
        }
    }

Run ``shopstats.py``

It will generate:

* ``shopstats_«month».csv``: the information extracted from the API.

  The name of the file includes a reference to the year and month of the
  corresponding period.

* different ``png`` files containing the charts, also noted with the
  reference about the period.

  At this moment, there are three charts included:

  - malformed: entries not containing the expected information

  - distinct: duplicated entries (for raw nodepoints)

  - billing: comparison of the aggregated billings of different nodepoints

Additionally
============

Once generated the ``shopstats*.csv`` file, it is possible to regenerate
the charts by calling the ``shopcharts.py`` script. It requires one
command line argument with the path to the ``csv``.

This script will try to get the period from the given filename.
