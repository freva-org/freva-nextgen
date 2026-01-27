Remote data access utilities
============================

The *freva-client* library allows to access any data format from anywhere as
zarr streams via http. This helps you to collaborate and gain visibility within
your community by making your datasets easily available to others without having
to copy any data.

No matter if your data is in GRIB, netCDF, geoTiff or even tar balls on tape
you can make them accessible from anywhere via secure zarr streams.
No pre-processing, like creating kerchunk files, is necessary.

Use the ``freva_client.zarr_utils`` module or the command line interface
``freva-client zarr-utils`` to get started.

Authentication
--------------
Since the data you want to access is in most cases not publicly you must
be authenticated in order to start making any zarr data available. Please check
out the :ref:`auth` section for more information.


Table of Content
----------------

.. toctree::
   :maxdepth: 2

   python-lib
   cli
