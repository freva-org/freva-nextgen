Databrowser python module
=========================
.. _python-databrowser:

.. toctree::
   :maxdepth: 3

The following section gives an overview over the usage of the databrowser
client python module. Please see the :ref:`install+configure` section on how
to install and configure the library.

TLDR: Too long didn't read
--------------------------
To query data databrowser and search for data you have three different options.
You can the to following methods

- :py:class:`freva_client.databrowser`: The main class for searching data is the
  :py:class:`freva_client.databrowser` class. After creating an instance of the
  databrowser class with your specific search constraints you can get retrieve
  all *files* or *uris* that match your search constraints. You can also
  retrieve a count of the objects matching the search, as well as
  getting an overview over the available metadata and creating an intake-esm
  catalogue from your search. Searching for *Uris* instead of *file* paths
  can be useful to get information on the storage system where the *files*
  or object stores are located.

- :py:meth:`freva_client.databrowser.metadata_search`: This class method lists
  all search categories (facets) and their values.

- :py:meth:`freva_client.databrowser.count_values`: You can count the
  occurrences of search results with this method.

- :py:meth:`freva_client.databrowser.userdata`: This calss method lets you
  *add* or *delete* your own metadata.

Library Reference
-----------------
Below you can find a more detailed documentation.

.. automodule:: freva_client
   :members: databrowser
