Databrowser python module
=========================
.. _python-zarr-utils:

.. toctree::
   :maxdepth: 3

The following section gives an overview over the usage of the available zarr
uitility python module. Please see the :ref:`install+configure` section on how
to install and configure the ``freva-client`` library.


Convert data to zarr
--------------------
With help of the :py:func:`freva_client.zarr_utils.convert` you can convert
and optionally aggregate your data to zarr.

.. autofunction:: freva_client.zarr_utils.convert

Check the status of a zarr store
---------------------------------

You can use the :py:func:`freva_client.zarr_utils.status`: to check the
status of a conversion job. This method can be useful is client tools like
xarray fail to open the remote zarr stores but don't give any describtive
error message. You can then simply occurrences of search results with this
method.


.. autofunction:: freva_client.zarr_utils.status
.. autoclass:: freva_client.utils.types.ZarrOptions
