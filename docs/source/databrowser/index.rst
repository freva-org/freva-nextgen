Search for data
===============

The *freva-databrowser* allows you to perform searches for climate datasets
in a wide range of programming languages. The databrowser offers a python
library `freva_databrowser` a command line interface (cli) - `freva-databrowser`
and a REST API which allows you to easily integrate the API into your
preferred language and environment.
Whether you use Python, JavaScript, R, Julia, or any other language with HTTP
request capabilities, the Freva Databrowser REST API accommodates your needs.

Getting Started
---------------
To begin using the *freva-databrowser*, you can make key-value
paired queries to search for specific datasets based on different
Data Reference Syntax (DRS) standards. The API currently supports
several standards, including ``CMIP5``, ``CMIP6``, ``CORDEX``, ``Freva``, and
``NextGEMS``. With the ability to combine these standards intelligently, the
databrowser provides a comprehensive and versatile search entry point.

Authentication
--------------
The Freva Databrowser is open and publicly accessible, and as such,
it doesn't require authentication. This means you can quickly start exploring
and accessing climate data without any additional setup.


Table of Content
----------------

.. toctree::
   :maxdepth: 2

   python-lib
   APIRef
   cli
