.. _intake:

``GET /intake_catalogue/`` Endpoint
------------------------------------

Generate an intake-esm catalogue from a data query.

Description
~~~~~~~~~~~
This endpoint generates an intake-esm catalogue in JSON format from a `freva`
search. The catalogue includes metadata about the datasets found in the search
results. Intake-esm is a data cataloging system that allows easy organization,
discovery, and access to Earth System Model (ESM) data. The generated catalogue
can be used by tools compatible with intake-esm, such as Pangeo.

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

If the request is successful, the server will respond with a JSON object
representing the intake-esm catalogue.

The catalogue will have the following structure:

- `esmcat_version`: The version of the intake-esm catalogue used (e.g., "0.1.0").
- `attributes`: A list of dictionaries containing information about the
  attributes/columns in the catalogue.
  - `column_name`: The name of the attribute/column.
  - `vocabulary`: The vocabulary associated with the attribute, if applicable.
- `assets`: A dictionary containing information about the assets/files in the catalogue.
  - `column_name`: The name of the column containing asset URLs.
  - `format_column_name`: The name of the column containing the file format information.
- `id`: The identifier of the catalogue (e.g., "freva").
- `description`: A description of the catalogue.
- `title`: The title of the catalogue.
- `last_updated`: The timestamp of the last update to the catalogue.
- `aggregation_control`: A dictionary containing information about aggregation
  options.
  - `variable_column_name`: The name of the column used for aggregating variables.
  - `groupby_attrs`: A list of attributes used for grouping datasets.
  - `aggregations`: A list of dictionaries representing aggregation options.
  - `type`: The type of aggregation (e.g., "union").
  - `attribute_name`: The name of the attribute to be aggregated.
  - `options`: Options specific to the aggregation type.
- `catalog_dict`: A list of dictionaries, each representing a dataset in the
  catalogue. Each dataset dictionary contains metadata about the dataset, such
  as file URLs, project, product, institute, model, experiment, time frequency,
  realm, variable, ensemble, CMOR table, file system type, and grid label.

Request
~~~~~~~~~

This endpoint does not require any additional request parameters.

Example
~~~~~~~

Here's an example of how to use this endpoint with additional parameters.
In this example we use the `freva` DRS standard and search for `file` entries.
Here we also want to get only those datasets that belong to the ``EUR-11``
``product``. Since intake is a python library we can only make direct use
of the intake catalogue in the python example.


.. tabs::

    .. code-tab:: bash
        :caption: cURL
        :emphasize-lines: 1

        curl -X GET 'http://api.freva.example/intake_catalogue/freva/file?product=EUR-11'

    .. code-tab:: python
        :caption: Python
        :emphasize-lines: 2

        import requests
        import intake
        response = requests.get(
            "http://api.freva.example/intake_catalogue/freva/file",
            pramas={"product": "EUR-11"}
        )
        cat = intake.open_esm_datastore(cat)

    .. code-tab:: r
        :caption: gnuR

        library(httr)
        response <- GET(
            "http://api.freva.example/intake_catalogue/freva/file",
            query = list(product = "EUR-11")
        )
        json_content <- content(response, "text", encoding="utf-8")
        write(json_content, file = "intake_catalogue.json")

    .. code-tab:: julia
        :caption: Julia

        using HTTP
        using JSON
        response = HTTP.get(
            "http://api.freva.example/intake_catalogue/freva/file",
            query = Dict("product" => "EUR-11")
        )
        data = JSON.parse(String(HTTP.body(response)))
        open("intake_catalogue.json", "w") do io
            write(io, JSON.json(data))
        end

    .. code-tab:: c
        :caption: C/C++

        #include <stdio.h>
        #include <curl/curl.h>

        int main() {
            CURL *curl;
            CURLcode res;
            FILE *fp;

            curl = curl_easy_init();
            if (curl) {
                char url[] = "http://api.freva.example/intake_catalogue/freva/file?product=EUR-11";
                curl_easy_setopt(curl, CURLOPT_URL, url);

                fp = fopen("intake_catalogue.json", "w");
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);

                res = curl_easy_perform(curl);
                if (res != CURLE_OK) {
                    printf("Error: %s\n", curl_easy_strerror(res));
                }

                curl_easy_cleanup(curl);
                fclose(fp);
            }
            return 0;
        }

    .. code-tab:: json
        :caption: JSON Response:

        {
             "esmcat_version": "0.1.0",
             "attributes": [
               {
                 "column_name": "project",
                 "vocabulary": ""
               },
               {
                 "column_name": "product",
                 "vocabulary": ""
               },
               {
                 "column_name": "institute",
                 "vocabulary": ""
               },
               // ... (other attributes)
             ],
             "assets": {
               "column_name": "uri",
               "format_column_name": "format"
             },
             "id": "freva",
             "description": "Catalogue from freva-databrowser v2023.4.1",
             "title": "freva-databrowser catalogue",
             "last_updated": "2023-07-26T10:50:18.592898",
             "aggregation_control": {
               // ... (aggregation options)
             },
             "catalog_dict": [
               {
                 "file": "https://swift.dkrz.de/v1/...",
                 "project": ["cordex"],
                 "product": ["EUR-11"],
                 "institute": ["GERICS"],
                 "model": ["NCC-NorESM1-M-GERICS-REMO2015-v1"],
                 "experiment": ["rcp85"],
                 "time_frequency": ["3hr"],
                 "realm": ["atmos"],
                 "variable": ["pr"],
                 "ensemble": ["r1i1p1"],
                 "cmor_table": ["3hr"],
                 "fs_type": "posix",
                 "grid_label": ["gn"]
               },
               // ... (other datasets)
             ]
           }

---

.. note::
   Please note that in these examples,
   I used "https://api.freva.example" as a placeholder URL.
   You should replace it with the actual URL of your
   Freva Databrowser REST API. The response above is truncated for brevity.
   The actual response will include more datasets in the `catalog_dict` list.
