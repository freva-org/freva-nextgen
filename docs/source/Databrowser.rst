.. _databrowser:

``GET /databrowser/`` Endpoint
------------------------------

Search for the locations of datasets.

Description
~~~~~~~~~~~~~~

This endpoint allows you to search for climate datasets based on the specified
Data Reference Syntax (DRS) standard (`flavour`) and the type of search result
(`uniq_key`), which can be either "file" or "uri". The `databrowser` method
provides a flexible and efficient way to query datasets matching specific search
criteria and retrieve a list of data files or locations that meet the query
parameters.

Parameters
~~~~~~~~~~~~

URL Parameters
################

- `flavour` (str): The Data Reference Syntax (DRS) standard specifying the type
  of climate datasets to query. The available DRS standards can be retrieved
  using the `search_attributes` GET method.
- `uniq_key` (str): The type of search result, which can be either "file" or
  "uri". This parameter determines whether the search will be based on file
  paths or Uniform Resource Identifiers (URIs).


Query Parameters
################

- `batch_size` (int, optional): Control the number of maximum items returned by the query. Default is 150.
- `start` (int, optional): Specify the starting point for receiving search results. Default is 0.
- `multi_version` (bool, optional): Use versioned datasets for querying instead  of the latest datasets. Default is True.

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

The response will be a streaming response in plain text format, providing a list
of data files or locations that match the search criteria.

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

        curl -X GET 'http://api.freva.example/databrowser/freva/file?product=EUR-11'

    .. code-tab:: python
        :caption: Python
        :emphasize-lines: 2

        import requests
        response = requests.get(
            "http://api.freva.example/databrowser/freva/file",
            pramas={"product": "EUR-11"}
        )
        data = list(response.iter_lines(decode_unicode=True))

    .. code-tab:: r
        :caption: gnuR
        :emphasize-lines: 2

        library(httr)
        response <- GET(
            "http://api.freva.example/databrowser/freva/file",
            query = list(product = "EUR-11")
        )
        data <- strsplit(content(response, as = "text", encoding = "UTF-8"), "\n")[[1]]



    .. code-tab:: julia
        :caption: Julia
        :emphasize-lines: 3

        using HTTP
        response = HTTP.get(
            "http://api.freva.example/facet_search/freva/file",
            query = Dict("product" => "EUR-11")
        )
        data = split(String(HTTP.body(response)),"\n")

    .. code-tab:: c
        :caption: C/C++
        :emphasize-lines: 10-13

        #include <stdio.h>
        #include <curl/curl.h>

        int main() {
            CURL *curl;
            CURLcode res;
            const char *url = "https://api.freva.example/databrowser/freva/file";

            // Query parameters
            const char *product = "EUR-11";
            const int batch_size = 50;
            const int start = 0;
            const int multi_version = 0; // 0 for false, 1 for true

            // Build the query string
            char query[256];
            snprintf(query, sizeof(query), "?product=%s&batch_size=%d&start=%d&multi_version=%d", product, batch_size, start, multi_version);

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




    .. code-tab:: Output
       :caption: Response:

       https://swift.dkrz.de/v1/dkrz_a32dc0e8-2299-4239-a47d-6bf45c8b0160/freva_test/model/
       regional/cordex/output/EUR-11/GERICS/NCC-NorESM1-M/rcp85/r1i1p1/GERICS-REMO2015/v1/
       3hr/pr/v20181212/pr_EUR-11_NCC-NorESM1-M_rcp85_r1i1p1_GERICS-REMO2015_v2_3hr_200701
       020130-200701020430.zarr
       https://swift.dkrz.de/v1/dkrz_a32dc0e8-2299-4239-a47d-6bf45c8b0160/freva_test/model/
       regional/cordex/output/EUR-11/CLMcom/MPI-M-MPI-ESM-LR/historical/r1i1p1/CLMcom-CCLM4-8-17/
       v1/day/tas/v20140515/tas_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_CLMcom-CCLM4-8-17_v1_
       day_194912011200-194912101200.zarr

---

The `databrowser` endpoint provides a powerful tool to search for climate
datasets based on various criteria. By using this method, you can efficiently
retrieve a list of data files or locations that match your specific requirements.
Make the most of the `databrowser` endpoint to access valuable climate data
effortlessly in the Freva Databrowser REST API!
