.. _search_facets:

``GET /facet_search/`` Endpoint
-------------------------------

Get the search parameters (facets) and count them.

Description
~~~~~~~~~~~

This endpoint allows you to search for facets and count them based on the
specified Data Reference Syntax (DRS) standard (`flavour`) and the type of
search result (`uniq_key`), which can be either `file` or `uri`.
Facets represent the metadata categories associated with the climate datasets,
such as experiment, model, institute, and more. This method provides a
comprehensive view of the available facets and their corresponding counts
based on the provided search criteria.

Parameters
~~~~~~~~~~~

URL Parameters
###############

- `flavour` (str): The Data Reference Syntax (DRS) standard specifying the type
  of climate datasets to query. The available DRS standards can be retrieved
  using the `search_attributes` GET method.

- `uniq_key` (str): The type of search result, which can be either
  "file" or "uri". This parameter determines whether the search will be
  based on file paths or Uniform Resource Identifiers (URIs).

Query Parameters
################

They control the search criteria for the datasets.
- `batch_size` (int, optional, default: 150): Control the number of maximum items returned by the query. Default is 150.
- `start` (int, optional, default: 0): Specify the starting point for receiving search results. Default is 0.
- `multi_version` (bool, optional, default: false): Use versioned datasets for querying instead of the latest datasets. Default is True.

Additional parameters control the search criteria for datasets.
Depending on the DRS standard those coulde be:

- `product`: The product identifier for the dataset.
- `institute`: The institute responsible for the dataset.
- `model`: The model used to generate the dataset.
- `variable`: The variable represented in the dataset.
- `time_frequency`: The time frequency of the dataset.
- `realm`: The realm (e.g., "atmos", "ocean") of the dataset.
- `experiment`: The experiment associated with the dataset.
- `ensemble`: The ensemble identifier for the dataset.
- `cmor_table`: The CMOR table identifier for the dataset.
- `grid_label`: The grid label for the dataset.
- `fs_type`: The file system type of the dataset (e.g., "posix", "swift").


Response Format
~~~~~~~~~~~~~~~~~~~

The response will be a JSON object containing the search facets and their
corresponding counts, along with a list of search results.

Request
~~~~~~~~~

This endpoint does not require any additional request parameters.

Example
~~~~~~~~~

Here's an example of how to use this endpoint with additional parameters.
In this example we use the `freva` DRS standard and search for `file` entries.
Here we also want to get only those datasets that belong to the ``EUR-11``
``product``

.. tabs::

    .. code-tab:: bash
        :caption: cURL
        :emphasize-lines: 1

        curl -X GET 'http://api.freva.example/facet_search/freva/file?product=EUR-11'


    .. code-tab:: python
        :caption: Python
        :emphasize-lines: 2

        import requests
        response = requests.get(
            "http://api.freva.example/facet_search/freva/file",
            pramas={"product": "EUR-11"}
        )
        data = response.json()

    .. code-tab:: r
        :caption: gnuR
        :emphasize-lines: 2

        library(httr)
        response <- GET(
            "http://api.freva.example/facet_search/freva/file",
            query = list(product = "EUR-11")
        )
        data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

    .. code-tab:: julia
        :caption: Julia
        :emphasize-lines: 3

        using HTTP
        using JSON
        response = HTTP.get(
            "http://api.freva.example/facet_search/freva/file",
            query = Dict("product" => "EUR-11")
        )
        data = JSON.parse(String(HTTP.body(response)))

    .. code-tab:: c
        :caption: C/C++
        :emphasize-lines: 10-13

        #include <stdio.h>
        #include <curl/curl.h>

        int main() {
            CURL *curl;
            CURLcode res;
            const char *url = "https://api.example.com/facet_search/freva/file";

            // Query parameters
            const char *product = "EUR-11";

            // Build the query string
            char query[256];
            snprintf(query, sizeof(query), "?product=%s", product);

            // Initialize curl
            curl = curl_easy_init();
            if (!curl) {
                fprintf(stderr, "Failed to initialize curl\n");
                return 1;
            }

            // Construct the full URL with query parameters
            char full_url[512];
            snprintf(full_url, sizeof(full_url), "%s%s", url, query);

            // Set the URL to fetch
            curl_easy_setopt(curl, CURLOPT_URL, full_url);

            // Perform the request
            res = curl_easy_perform(curl);
            if (res != CURLE_OK) {
                fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            }

            // Clean up
            curl_easy_cleanup(curl);

            return 0;
        }


    .. code-tab:: json
        :caption: JSON Response:

        {
           "total_count": 7,
           "facets": {
               "cmor_table": ["1day", "3", "3hr", "3", "fx", "1"],
               "dataset": ["cordex-fs", "3", "cordex-hsm", "2", "cordex-swfit", "2"],
               "driving_model": ["mpi-m-mpi-esm-lr", "4", "ncc-noresm1-m", "3"],
               "ensemble": ["r0i0p0", "1", "r1i1p1", "6"],
               "experiment": ["historical", "4", "rcp85", "3"],
               "format": ["nc", "5", "zarr", "2"],
               "fs_type": ["posix", "7"],
               "grid_id": [],
               "grid_label": ["gn", "7"],
               "institute": ["clmcom", "4", "gerics", "3"],
               "level_type": ["2d", "7"],
               "model": ["mpi-m-mpi-esm-lr-clmcom-cclm4-8-17-v1", "4", "ncc-noresm1-m-gerics-remo2015-v1", "3"],
               "product": ["eur-11", "7"],
               "project": ["cordex", "7"],
               "rcm_name": ["clmcom-cclm4-8-17", "4", "gerics-remo2015", "3"],
               "rcm_version": ["v1", "7"],
               "realm": ["atmos", "7"],
               "time_aggregation": ["avg", "7"],
               "time_frequency": ["1day", "3", "3hr", "3", "fx", "1"],
               "variable": ["orog", "1", "pr", "3", "tas", "3"]
           },
           "search_results": [
               {"file": "https://swift.dkrz.de/...", "fs_type": "p"},
               {"file": "https://swift.dkrz.de/...", "fs_type": "p"},
               {"file": "/home/wilfred/workspace/...", "fs_type": "p"},
               {"file": "/home/wilfred/workspace/...", "fs_type": "p"},
               {"file": "/home/wilfred/workspace/...", "fs_type": "p"},
               {"file": "/arch/bb1203/...", "fs_type": "p"},
               {"file": "/arch/bb1203/...", "fs_type": "p"}
           ],
           "facet_mapping": {
               "experiment": "experiment",
               "ensemble": "ensemble",
               "fs_type": "fs_type",
               "grid_label": "grid_label",
               "institute": "institute",
               "model": "model",
               "project": "project",
               "product": "product",
               "realm": "realm",
               "variable": "variable",
               "time_aggregation": "time_aggregation",
               "time_frequency": "time_frequency",
               "cmor_table": "cmor_table",
               "dataset": "dataset",
               "driving_model": "driving_model",
               "format": "format",
               "grid_id": "grid_id",
               "level_type": "level_type",
               "rcm_name": "rcm_name",
               "rcm_version": "rcm_version"
           },
           "primary_facets": ["experiment", "ensemble", "institute", "model", "project", "product", "realm", "time_aggregation", "time_frequency"]
        }

---

.. note::
   Please note that in these examples,
   I used "https://api.freva.example" as a placeholder URL.
   You should replace it with the actual URL of your
   Freva Databrowser REST API.
