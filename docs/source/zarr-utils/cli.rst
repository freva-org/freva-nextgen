The zarr utils command line interface
=====================================
.. _zarr-utils-cli:

.. toctree::
   :maxdepth: 3

This section introduces the usage of the ``freva-client zarr-utils`` sub command.
Please see the :ref:`install+configure` section on how to install and
configure the command line interface.


After successful installation you can use the ``freva-client zarr-utils`` sub
command

.. code:: console

    freva-client zarr-utils --help


Converting your data files
--------------------------
You can convert any data files on the data center to zarr stores and serve them
via http(s). To do so use the ``convert`` sub-command:

.. code:: console

    freva-client zarr-utils convert --help

.. execute_code::
   :hide_code:

   from subprocess import run, PIPE
   res = run(["freva-client", "zarr-utils", "convert", "--help"], check=True, stdout=PIPE, stderr=PIPE)
   print(res.stdout.decode())


The ``--public`` flag lets you create zarr stores that can be anonymously acceesed.
The ``aggregation`` flags can be used to aggregate multiple source file into one common zarr store.

.. warning::

    Anyone with this link will be able to access the data. For security reasons
    any public zarr store link has an expiration date after which access isn't
    possible anymore.


Checking the conversion status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If client tools, like xarray, that access the zarr stores file you can use the
``status`` sub-command to get more information about the loading process and
potential issues.

.. code:: console

    freva-client zarr-utils status --help

.. execute_code::
   :hide_code:

   from subprocess import run, PIPE
   res = run(["freva-client", "zarr-utils", "status", "--help"], check=True, stdout=PIPE, stderr=PIPE)
   print(res.stdout.decode())
