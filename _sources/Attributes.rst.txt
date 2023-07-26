.. _search_attributes:

``GET /search_attributes`` Endpoint
-----------------------------------

Get all available search flavours and their attributes.

Description
~~~~~~~~~~~

This endpoint allows you to retrieve an overview of the different
Data Reference Syntax (DRS) standards implemented in the Freva Databrowser
REST API. The DRS standards define the structure and metadata organisation
for climate datasets, and each standard offers specific attributes for
searching and filtering datasets.

Response Format
~~~~~~~~~~~~~~~

The response will be a JSON object containing a list of available search
flavours and their corresponding attributes.

Request
~~~~~~~

This endpoint does not require any specific request parameters.

Examples
~~~~~~~~

.. tabs::

    .. code-tab:: bash
        :caption: cURL
        :emphasize-lines: 1

        curl -X GET http://api.freva.example/search_attributes

    .. code-tab:: python
        :caption: Python
        :emphasize-lines: 2

        import requests
        response = requests.get("http://api.freva.example/search_attributes")
        data = response.json()

    .. code-tab:: r
        :caption: gnuR
        :emphasize-lines: 2

        library(httr)
        response <- GET("http://api.freva.example/search_attributes")
        data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

    .. code-tab:: julia
        :caption: Julia
        :emphasize-lines: 3

        using HTTP
        using JSON
        response = HTTP.get("http://api.freva.example/search_attributes")
        data = JSON.parse(String(HTTP.body(response)))

    .. code-tab:: c
        :caption: C/C++
        :emphasize-lines: 10-13

        #include <stdio.h>
        #include <curl/curl.h>

        int main() {
            CURL *curl;
            CURLcode res;

            curl = curl_easy_init();
            if (curl) {
                char url[] = "https://api.freva.example/search_attributes";

                curl_easy_setopt(curl, CURLOPT_URL, url);
                res = curl_easy_perform(curl);
                curl_easy_cleanup(curl);
            }

            return 0;
        }

    .. code-tab:: json
        :caption: JSON Response:

        {
          "flavours": [
            "freva",
            "cmip6",
            "cmip5",
            "cordex",
            "nextgems"
          ],
          "attributes": {
            "freva": [
              "experiment",
              "ensemble",
              "fs_type",
              "grid_label",
              "institute",
              "model",
              "project",
              "product",
              "realm",
              "variable",
              "time_aggregation",
              "time_frequency",
              "cmor_table",
              "dataset",
              "format",
              "grid_id",
              "level_type"
            ],
            "cmip6": [
              "experiment_id",
              "member_id",
              "fs_type",
              "grid_label",
              "institution_id",
              "source_id",
              "mip_era",
              "activity_id",
              "realm",
              "variable_id",
              "time",
              "time_aggregation",
              "frequency",
              "table_id",
              "dataset",
              "format",
              "grid_id",
              "level_type"
            ],
            "cmip5": [
              "experiment",
              "member_id",
              "fs_type",
              "grid_label",
              "institution_id",
              "model_id",
              "project",
              "product",
              "realm",
              "variable",
              "time",
              "time_aggregation",
              "time_frequency",
              "cmor_table",
              "dataset",
              "format",
              "grid_id",
              "level_type"
            ],
            "cordex": [
              "experiment",
              "ensemble",
              "fs_type",
              "grid_label",
              "institution",
              "model",
              "project",
              "domain",
              "realm",
              "variable",
              "time",
              "time_aggregation",
              "time_frequency",
              "cmor_table",
              "dataset",
              "driving_model",
              "format",
              "grid_id",
              "level_type",
              "rcm_name",
              "rcm_version"
            ],
            "nextgems": [
              "experiment",
              "member_id",
              "fs_type",
              "grid_label",
              "institution_id",
              "source_id",
              "project",
              "experiment_id",
              "realm",
              "variable_id",
              "time",
              "time_reduction",
              "time_frequency",
              "cmor_table",
              "dataset",
              "format",
              "grid_id",
              "level_type"
            ]
          }
        }

---

.. note::
   Please note that in these examples,
   I used "https://api.freva.example" as a placeholder URL.
   You should replace it with the actual URL of your
   Freva Databrowser REST API.

These examples demonstrate how you can use various programming languages to
make a GET request to the search_attributes endpoint and receive the
JSON response containing the available search flavours and their attributes.
