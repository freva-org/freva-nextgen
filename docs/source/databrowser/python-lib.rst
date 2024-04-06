Databrowser python module
-------------------------
.. _databrowser:

.. toctree::
   :maxdepth: 3

The following section gives an overview over the usage of the databrowser
client python module. First we will briefly explain how the `freva-databrowser`
package can be installed and configured (if needed). Then we will have an
overview over the usage and the different methods of the `freva-databrowser`
package.

Installation
============
Installation of the databrowser client library is straight forward and can be
achieved via:

.. code:: console

    python3 -m pip install freva-databrowser

After successful installation you can import the `databrowser` class from the
`freva_databrowser` package:

.. code:: python

    from freva_databrowser import databrowser


Within the


TLDR: Too long didn't read
==========================
To query data databrowser and search for data you have three different options.
You can the to following methods

- :py:class:`databrowser`: The main class for searching data is the
  :py:class:`freva.databrowser` class. After creating in instance of the
  databrowser class with your specific search constraints you can get retrieve
  all *files* or *uris* that matching your search constraints. You can also
  retrieve a count of the number objects matching the search, as well as
  getting an overview over the available metadata and creating an intake-esm
  catalogue from your search. Searching for *Uris* instead of *file* paths
  can be useful to get information on the storage system where the *files*
  or object stores are located.

- :py:meth:`databrowser.metadata_search`: This class method lists all search
  categories (facets) and their values.

- :py:meth:`freva.count_values`: You can count the occurrences of
  search results with this method.


Below you can find a more detailed documentation.

.. automodule:: freva_databrowser
   :members: databrowser

Configuration
=============
You can either tell the library to connect to a specifc databrowser server,
such as www.freva.dkrz.de or use a configuration file to permanently store
the location of this server. This configuration
is defined the ``freva.toml`` config file. This file follows
`toml syntax <https://toml.io/en/>`_. You can either set the environment
variable ``FREVA_CONFIG`` to the correct location of the ``freva.toml`` file
or use your *user config directory* such as ``~/.config/freva/freva.toml``.
System wide configuration is also possible by the global data location of your
current python environment. After installation the config file will be installed
in ``<ENV_PATH>/share/freva/freva.toml`` where ``<ENV_PATH>`` is the path to your
current python environment. All you need to do is setting the ``databrowser_host``
key in the ``[freva]`` section.
