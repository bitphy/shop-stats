##########################################
Reporting of shop stats for the Bitphy API
##########################################

This project consist of some scripts that extract information
from the BitPhy API and generates some charts to check for consistency.

To use it:

Create a file named bitphyaccess.json in src/ containing:

::

    {
        "url_base":  "http://api.dev.bitphy.es/v2.0",
        "headers": {
            "Authorization": "Bearer «place here your access tocken»",
            "Cache-Control": "no-cache"
        }
    }

Run ``shopstats.py``

It will generate:

* ``shopstats_«month».csv``: the information extracted from the API

*  different ``png`` files containing the charts
