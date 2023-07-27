.. freva-databrowser-api documentation master file, created by
   sphinx-quickstart on Tue Jul 25 17:08:08 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the Freva Databrowser REST API Documentation
=======================================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. image:: https://img.shields.io/badge/License-BSD-purple.svg
   :target: LICENSE

.. image:: https://img.shields.io/badge/python-3.11-purple.svg
   :target: https://www.python.org/downloads/release/python-311/

.. image:: https://img.shields.io/badge/poetry-1.5.1-purple
   :target: https://python-poetry.org/

.. image:: https://img.shields.io/badge/ViewOn-GitHub-purple
   :target: https://github.com/FREVA-CLINT/databrowserAPI

.. image:: https://github.com/FREVA-CLINT/databrowserAPI/actions/workflows/ci_job.yml/badge.svg
   :target: https://github.com/FREVA-CLINT/databrowserAPI/actions

.. image:: https://codecov.io/github/FREVA-CLINT/databrowserAPI/branch/init/graph/badge.svg?token=dGhXxh7uP3
   :target: https://codecov.io/github/FREVA-CLINT/databrowserAPI


The Freva Databrowser REST API is a powerful tool that enables you to search
for climate and environmental datasets seamlessly in various programming
languages. By generating RESTful requests, you can effortlessly access
collections of various datasets, making it an ideal resource for
climate scientists, researchers, and data enthusiasts.

Getting Started
---------------
To begin using the Freva Databrowser REST API, you can make key-value
paired queries to search for specific datasets based on different
Data Reference Syntax (DRS) standards. The API currently supports
several standards, including ``CMIP5``, ``CMIP6``, ``CORDEX``, ``Freva``, and
``NextGEMS``. With the ability to combine these standards intelligently, the
API provides a comprehensive and versatile search entry point.

Searching for Climate Datasets
------------------------------
The API's flexible design allows you to perform searches for climate datasets
in a wide range of programming languages. By generating RESTful requests,
you can easily integrate the API into your preferred language and environment.
Whether you use Python, JavaScript, R, Julia, or any other language with HTTP
request capabilities, the Freva Databrowser REST API accommodates your needs.

Specific ``GET`` Methods
------------------------
The Freva Databrowser REST API offers specific ``GET`` methods that allow you
to fine-tune your searches and retrieve tailored results. These methods include:

- Search by Key-Value Pairs: Use the ``GET /databrowser`` endpoint to
  make key-value paired queries and find climate datasets that match your
  specific criteria. This method enables you to target datasets based on
  parameters such as `variables`, `models`, `time ranges`, and `experiments`.

- Query All Facets: The ``GET /search_facets`` endpoint allows you to query
  all available facets of the indexed data. By exploring facets like
  `models`, `experiments`, and `institutions`, you can gain valuable insights
  into the dataset's metadata, assisting you in refining your search effectively.
  You can also count the number of datasets available within each facet.
  This feature provides an overview of the dataset distribution across various
  metadata categories, aiding you in identifying the most relevant data for
  your research.

- Generating Intake-ESM Catalogues: The ``GET /intake_catalogue`` lets you seamlessly
  generate Intake-ESM catalogues from your search queries.
  Intake-ESM is a powerful data cataloging tool widely used in Earth System
  Model (ESM) analysis. This integration allows you to create curated
  catalogues, streamlining your data analysis workflow.

How to Use the Documentation
----------------------------
This user documentation serves as a comprehensive guide to effectively using
the Freva Databrowser REST API in various programming languages. It provides
detailed explanations of available endpoints, query parameters,
and response formats. Whether you're an experienced developer or a novice user,
you'll find step-by-step instructions and examples to leverage the API's full
potential.

Authentication
---------------
The Freva Databrowser REST API is open and publicly accessible, and as such,
it doesn't require authentication. This means you can quickly start exploring
and accessing climate data without any additional setup.

Feedback and Support
---------------------
We value your feedback and are committed to improving the API continuously.
If you encounter any issues, have questions, or wish to suggest improvements,
please don't hesitate to reach out to our support team. Your input is
invaluable in enhancing the Freva Databrowser REST API to better suit your
needs.

Conclusion
----------
The Freva Databrowser REST API opens up a world of possibilities for
effortlessly searching and accessing climate datasets in various
programming languages. With its RESTful nature and support for
different DRS standards, specific GET methods, querying facets and generating
Intake-ESM catalogues, you can efficiently navigate and analyse the vast
climate data landscape.


.. toctree::
   :maxdepth: 2
   :caption: Content

   APIRef


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
