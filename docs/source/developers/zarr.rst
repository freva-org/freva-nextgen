Zarr Streaming API
==================
Definition of endpoints for loading/streaming and manipulating data.

This API exposes asynchronous services that convert files or objects into Zarr
stores and stream them to clients.  The conversion is performed by a
background worker and communicated via a message broker; the REST endpoints
return references (URLs) to the resulting Zarr datasets rather than the data
itself.

---

.. _databrowser-api-zarr:

Creating zarr endpoints for streaming data
-------------------------------------------

.. http:get:: /api/freva-nextgen/databrowser/load/(str:flavour)

   This endpoint searches for datasets and streams the results as Zarr data.
   The Zarr format allows for efficient storage and retrieval of large,
   multidimensional arrays. This endpoint can be used to query datasets and
   receive the results in a format that is suitable for further analysis and
   processing with Zarr. If the ``catalogue-type`` parameter is set to "intake",
   it can generate Intake-ESM catalogues that point to the generated Zarr
   endpoints.

   :param flavour: The Data Reference Syntax (DRS) standard specifying the
                   type of climate datasets to query. The available
                   DRS standards can be retrieved using the
                   ``GET /api/datasets/overview`` method.
   :type flavour: str
   :query start: Specify the starting point for receiving search results.
                Default is 0.
   :type start: int
   :type max-results: int
   :query multi-version: Use versioned datasets for querying instead of the
                         latest datasets. Default is false.
   :type multi-version: bool
   :query translate: Translate the metadata output to the required DRS flavour.
                     Default is true
   :type translate: bool
   :query catalogue-type: Set the type of catalogue you want to create from
                          this query.
   :type catalogue-type: str
   :query public:  Indicate whether you want to create a publicly available
                   temporary zarr url. Be default users need to be authenticated
                   in order to access the zarr urls. Default is false
   :type public: bool
   :query ttl_seconds: Set for how many seconds a the public zarr url should be
                       valid for, if any. Default is 86,400 (1 day).
   :query \**search_facets: With any other query parameters you refine your
                            data search. Query parameters could be, depending
                            on the DRS standard flavour ``product``, ``project``
                            ``model`` etc.
   :type \**search_facets: str, list[str]
   :reqheader Authorization: Bearer token for authentication.
   :reqheader Content-Type: application/json

   :statuscode 200: no error
   :statuscode 400: no entries found for this query
   :statuscode 422: invalid query parameters
   :resheader Content-Type: ``text/plain``: zarr endpoints for the data


   Example Request
   ~~~~~~~~~~~~~~~

   The logic works just like for the ``data-search`` and ``intake-catalogue``
   endpoints. We constrain the data search by ``key=value`` search pairs.
   The only difference is that we have to authenticate by using an access token.
   You will also have to use a valid access token if you want to access the
   zarr data via http. Please refer to the :ref:`auth` chapter for more details.

   .. sourcecode:: http

       GET /api/freva-nextgen/databrowser/load/freva/file?dataset=cmip6-fs HTTP/1.1
       Host: www.freva.dkrz.de
       Authorization: Bearer your_access_token

   Example Response
   ~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 200 OK
       Content-Type: plain/text

       https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/dcb608a0-9d77-5045-b656-f21dfb5e9acf.zarr
       https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/f56264e3-d713-5c27-bc4e-c97f15b5fe86.zarr


   Example
   ~~~~~~~
   Below you can find example usages of this request in different scripting and
   programming languages.

   .. tabs::

       .. code-tab:: bash
           :caption: Shell

           curl -X GET \
           'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva?dataset=cmip6-fs'
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

       .. code-tab:: python
           :caption: Python

           import requests
           import intake
           response = requests.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva",
               params={"dataset": "cmip6-fs"},
               headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"},
               stream=True,
           )
           files = list(res.iterlines(decode_unicode=True)

       .. code-tab:: r
           :caption: gnuR

           library(httr)
           response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva",
               query = list(dataset = "cmip6-fs")
           )
           data <- strsplit(content(response, as = "text", encoding = "UTF-8"), "\n")[[1]]


       .. code-tab:: julia
           :caption: Julia

           using HTTP
           response = HTTP.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva",
               query = Dict("dataset" => "cmip6-fs")
           )
           data = split(String(HTTP.body(response)),"\n")

       .. code-tab:: c
           :caption: C/C++

           #include <stdio.h>
           #include <curl/curl.h>

           int main() {
               CURL *curl;
               CURLcode res;
               const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva";

               // Query parameters
               const char *dataset = "cmip6-fs";
               const int start = 0;
               const int multi_version = 0; // 0 for false, 1 for true

               // Build the query string
               char query[256];
               snprintf(query, sizeof(query),
                   "?dataset=%s&start=%d&multi-version=%d",product , start, multi_version);

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



.. _zarr_convert:

Request asynchronous Zarr conversion
------------------------------------

.. http:post:: /api/freva-nextgen/data-portal/zarr/convert

   Submit one or more file or object paths to be converted into Zarr stores.
   This endpoint only publishes a message to the data‑portal worker via a broker;
   it does **not** verify that the paths exist or perform the conversion itself.
   It returns a JSON object with a ``urls`` array; each entry contains a UUID
   that identifies the future Zarr dataset.

   If the data‑loading service cannot access a file, it will record the failure
   and the corresponding Zarr dataset will be in a failed state with a reason.
   You can query the status endpoint to check whether the conversion succeeded
   or failed.

   :<json string path: Absolute or object‑store paths to the input files. Can be a single
               path or a list of paths.
   :<json boolean public: Boolean indicating whether or not the zarr store that should
                 be created will be anonymously available.
   :<json integer ttl_seconds: How long should a pulic URL should remain valid, in seconds.
   :<json string aggregate: The string indicating how the aggregation should be done:
        - null/omit: Do not aggregate data (default)
        - "auto": let the system choose if the datasets should be concatenated or mereged.
        - "merge": merge datasets as `variables`
        - "concat": concatenated datasets along a `dimension`
   :<json string join: String indicating how to combine differing indexes:

        - "outer": use the union of object indexes.
        - "inner": use the intersection of object indexes.
        - "left": use indexes from the first object with each dimension.
        - "right": use indexes from the last object with each dimension.
        - "exact": instead of aligning, errors when indexes to be aligned are not equal.

        This option is only taken into account it ``aggregate`` is set
   :<json string compat: String indicating how to compare non-concatenated variables of the same name for:

        - "equals": all values and dimensions must be the same.
        - "no_conflicts": only values which are not null in both datasets
          must be equal. The returned dataset then contains the combination
          of all non-null values.
        - "override": skip comparing and pick variable from first dataset

        This option is only taken into account it ``aggregate`` is set.
   :<json string data_vars: These data variables will be combined together:

        - "minimal": Only data variables in which the dimension already appears are included.
        - "different":  Data variables which are not equal (ignoring attributes) across all datasets are also concatenated (as well as all for which dimension already appears).
        - "all": All data variables will be concatenated.

        This option is only taken into account it ``aggregate`` is set.
   :<json string coords: These coordinate variables will be combined together:

        - "minimal": Only coordinates in which the dimension already appears are included.
        - "different":  Coordinates which are not equal (ignoring attributes) across all datasets are also concatenated (as well as all for which dimension already appears).
        - "all": All coordinates will be concatenated.

        This option is only taken into account it ``aggregate`` is set.
   :<json string dim:  Name of the dimension to concatenate along. This can either be a new
                dimension name, in which case it is added along axis=0, or an
                existing dimension name, in which case the location of the
                dimension is unchanged.

                This option is only taken into account it ``aggregate`` is set.
   :<json string group_by: If set, forces grouping by a signature key. Otherwise grouping
                    is attempted only when direct combine fails.
   :reqheader Authorization: Bearer token for authentication.
   :statuscode 200: JSON object with a ``urls`` array containing the Zarr endpoint URLs.
   :statuscode 401: The user could not be authenticated.
   :statuscode 503: Service is currently unavailable.
   :statuscode 500: Internal error while publishing the data request.
   :resheader Content-Type: ``application/json``

   Example Request
   ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/data-portal/zarr/convert?path=/work/abc123/myuser/mydata_1.nc&path=/work/abc123/myuser/mydata_2.nc HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN

   Example Response
   ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "urls": [
                "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/907f6bca-1234-5678-9abc-def012345678.zarr",
                "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/aa8432de-9abc-4567-def0-123456789abc.zarr"
            ]
        }

   Examples
   ~~~~~~~~

    Below are example usages of this request in different languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X POST \
              'https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert' \
              --header "Authorization: Bearer YOUR_ACCESS_TOKEN" \
              --data-urlencode 'path=/work/abc123/myuser/mydata_1.nc' \
              --data-urlencode 'path=/work/abc123/myuser/mydata_2.nc'

        .. code-tab:: python
            :caption: Python (requests)

            import requests

            response = requests.post(
                "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert",
                json={
                    "path": [
                        "/work/abc123/myuser/mydata_1.nc",
                        "/work/abc123/myuser/mydata_2.nc",
                    ]
                },
                headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"},
            )
            zarr_locations = response.json()["urls"]

        .. code-tab:: r
            :caption: gnuR (httr)

            library(httr)
            library(jsonlite)

            response <- POST(
              "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert",
              query = list(path = c(
                "/work/abc123/myuser/mydata_1.nc",
                "/work/abc123/myuser/mydata_2.nc"
              )),
              add_headers(Authorization = "Bearer YOUR_ACCESS_TOKEN")
            )
            zarr_locations <- fromJSON(content(response, as = "text", encoding = "UTF-8"))$urls

        .. code-tab:: julia
            :caption: Julia (HTTP.jl)

            using HTTP
            using JSON

            headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
            query = Dict("path" => [
                "/work/abc123/myuser/mydata_1.nc",
                "/work/abc123/myuser/mydata_2.nc",
            ])
            response = HTTP.post(
              "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert";
              headers = headers,
              query = query,
            )

Generic Zarr key retrieval
--------------------------

In addition to the specific metadata and chunk routes listed above, the
data portal also exposes a catch‑all endpoint that serves arbitrary keys
from a Zarr store.  This interface is compatible with remote storage
backends used by :mod:`xarray` and :mod:`zarr`, allowing clients to
access consolidated metadata, per‑variable metadata and individual chunk
payloads via HTTP.

.. http:get:: /api/freva-nextgen/data-portal/zarr/{token}.zarr/{zarr_key}
    :synopsis: Retrieve arbitrary metadata or chunk from a remote Zarr store

    Retrieve metadata or chunk data from the store identified by ``token``.
    The ``zarr_key`` parameter must contain the slash‑separated key within
    the Zarr store.  The following conventions apply:

    * Root‑level keys such as ``.zmetadata``, ``.zgroup`` and ``.zattrs``
      return the consolidated metadata, group information and attributes
      for the entire store.  These keys have no variable prefix.
    * Keys of the form ``<variable>/.zarray`` and ``<variable>/.zattrs``
      return the array encoding and variable attributes for the specified
      variable.  Replace ``<variable>`` with the actual variable name.
    * Data chunk keys use the pattern ``<variable>/<chunk>`` where
      ``chunk`` encodes the chunk indices separated by dots.  For
      example, ``tas/0.0.0`` requests the first chunk of the variable
      ``tas``.

    :param token: Unique identifier returned by the load endpoint when a
       dataset has been registered for streaming.  Each token corresponds
       to one Zarr store.
    :param zarr_key: Slash‑separated key within the Zarr store.  Refer
       to the descriptions above for valid patterns.

    :status 200: The requested metadata or chunk was found and is
       returned in the response body.
    :status 400: The key is malformed or refers to a metadata file that
       requires a variable context (for example ``foo/.zarray`` at the
       root level).
    :status 404: The requested key does not exist in the store.

    **Example**

    To retrieve the consolidated metadata for a store, use ``.zmetadata``
    as the key::

        curl -X GET \
            "https://api.freva.de/api/freva-nextgen/data-portal/zarr/<token>.zarr/.zmetadata"

    You can open a remote store directly in :mod:`xarray` using the
    :mod:`fsspec` interface::

        import fsspec
        import xarray as xr

        mapper = fsspec.get_mapper(
            "https://api.freva.de/api/freva-nextgen/data-portal/zarr/<token>.zarr"
        )
        dset = xr.open_zarr(mapper, consolidated=True)
        dset.load()

.. http:get:: /api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/{zarr_key}
    :synopsis: Retrieve arbitrary metadata or chunk from a shared Zarr store

    This endpoint mirrors the behaviour of the previous one but requires
    a valid ``sig`` parameter obtained from :http:post:`/api/freva-nextgen/data-portal/zarr/share-zarr`.
    The signature authenticates the request and maps to the underlying token.

    **Example**

    Retrieve the consolidated metadata for a shared store::

        curl -X GET \
            "https://api.freva.de/api/freva-nextgen/data-portal/share/<sig>/<token>.zarr/.zmetadata"
            zarr_locations = JSON.parse(String(response.body))["urls"]

        .. code-tab:: c
            :caption: C (libcurl)

            #include <stdio.h>
            #include <curl/curl.h>

            int main(void) {
                CURL *curl = curl_easy_init();
                if (curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    // Note: encode special characters in the paths as needed
                    curl_easy_setopt(curl, CURLOPT_URL,
                        "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert"
                        "?path=/work/abc123/myuser/mydata_1.nc&"
                        "path=/work/abc123/myuser/mydata_2.nc");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                    CURLcode res = curl_easy_perform(curl);
                    curl_slist_free_all(headers);
                    curl_easy_cleanup(curl);
                }
                return 0;
            }

---

.. _zarr_public:

Create a public pre-signed zarr url
-----------------------------------

.. http:post:: /api/freva-nextgen/data-portal/zarr/share-zarr

   Create a short-lived, shareable pre-signed URL for a specific Zarr
   chunk. The caller must authenticate with a normal OAuth2 access
   token.

   The returned URL includes `expires` and `sig` query
   parameters. Anyone who knows the URL can perform a ``GET`` request on
   the target resource until the expiry time is reached, without
   needing an access token.



   :<json string path: Fully qualified URL of the resource to pre-sign, relative
               to this API. Must contain  `/api/freva-nextgen/data-portal/zarr/`
               and typically points to a single Zarr url.
   :<json integer ttl_seconds: How long the pre-signed URL should remain valid,
                     in seconds.
   :reqheader Authorization: Bearer token for authentication.
   :statuscode 200: JSON object with a ``url`` containing the Zarr endpoint URLs.
   :statuscode 401: The user could not be authenticated.
   :statuscode 503: Service is currently unavailable.
   :statuscode 500: Internal error while publishing the data request.
   :resheader Content-Type: ``application/json``

   Example Request
   ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/data-portal/share-zarr/?path=https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/432a5670.zarr HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN

   Example Response
   ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "url": "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/share/hRNJcEug/breezy-maxwell.zarr",
            "sig": "AbCdEf",
            "token": "123e4567",
            "expires": 1763540778,
            "method": "GET",
        }

   Examples
   ~~~~~~~~

    Below are example usages of this request in different languages.

    .. tabs::

      .. code-tab:: bash
         :caption: Shell

         curl -X POST \
           'https://www.freva.dkrz.de/api/freva-nextgen/data-portal/share-zarr\
           ?path=https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/432a5670.zarr' \
           -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

      .. code-tab:: python
         :caption: Python (requests)

         import requests

         response = requests.post(
             "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/share-zarr",
             json={
                 "path": (
                     "https://www.freva.dkrz.de/api/freva-nextgen/"
                     "data-portal/zarr/432a5670.zarr"
                 ),
             },
             headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"},
         )
         public_zarr = response.json()["url"]

      .. code-tab:: r
         :caption: gnuR (httr)

         library(httr)
         library(jsonlite)

         response <- POST(
           "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/share-zarr",
           query = list(
             path = "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/432a5670.zarr"
           ),
           add_headers(Authorization = "Bearer YOUR_ACCESS_TOKEN")
         )
         public_zarr <- fromJSON(
           content(response, as = "text", encoding = "UTF-8")
         )$url

      .. code-tab:: julia
         :caption: Julia (HTTP.jl)

         using HTTP
         using JSON

         headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
         response = HTTP.post(
           "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/share-zarr";
           headers = headers,
           query = Dict(
             "path" =>
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/432a5670.zarr",
           ),
         )
         public_zarr = JSON.parse(String(response.body))["url"]

      .. code-tab:: c
         :caption: C (libcurl)

         #include <stdio.h>
         #include <curl/curl.h>

         int main(void) {
             CURL *curl = curl_easy_init();
             if (curl) {
                 CURLcode res;
                 struct curl_slist *headers = NULL;

                 headers = curl_slist_append(
                     headers,
                     "Authorization: Bearer YOUR_ACCESS_TOKEN"
                 );

                 curl_easy_setopt(
                     curl,
                     CURLOPT_URL,
                     "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/"
                     "share-zarr?path=https://www.freva.dkrz.de/api/freva-nextgen/"
                     "data-portal/zarr/432a5670.zarr"
                 );
                 curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                 curl_easy_setopt(curl, CURLOPT_POST, 1L);

                 res = curl_easy_perform(curl);

                 curl_slist_free_all(headers);
                 curl_easy_cleanup(curl);
             }
             return 0;
         }

---

.. _databrowser-api-zarr-status:

Checking the status of a dynamically created Zarr dataset
---------------------------------------------------------

.. http:get:: /api/freva-nextgen/data-portal/zarr-utils/status

   This endpoint returns the current status of a dynamically created Zarr
   dataset. After submitting an instruction to generate a Zarr dataset,
   clients can poll this endpoint to track the progress of the conversion
   process. The response includes a machine-readable integer status code
   and a human-readable explanation.

   :reqheader Authorization: Bearer token for authentication.
   :reqheader Content-Type: application/json

   :statuscode 200: Status returned successfully.
   :statuscode 401: Unauthorised / invalid or missing access token.
   :statuscode 404: The token is not known to the system.
   :statuscode 503: The service is currently unavailable.

   :resheader Content-Type: ``application/json``

   Example Request
   ~~~~~~~~~~~~~~~

   To check the status of a dataset conversion, simply perform a ``GET`` request
   with a valid access token:

   .. sourcecode:: http

       GET /api/freva-nextgen/data-portal/zarr-utils/status HTTP/1.1
       Host: www.freva.dkrz.de
       Authorization: Bearer your_access_token

   Example Response
   ~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 200 OK
       Content-Type: application/json

       {
           "status": 2,
           "reason": "Dataset is currently being prepared"
       }

   Example
   ~~~~~~~

   Below you can find example usages of this request in different scripting and
   programming languages.

   .. tabs::

       .. code-tab:: bash
           :caption: Shell

           curl -X GET \
             'https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/status' \
             -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

       .. code-tab:: python
           :caption: Python

           import requests

           response = requests.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/status",
               headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"},
           )
           status = response.json()
           print(status["status"], status["reason"])

       .. code-tab:: r
           :caption: gnuR

           library(httr)
           response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/status",
               add_headers(Authorization = "Bearer YOUR_ACCESS_TOKEN")
           )
           data <- content(response, as = "parsed")

       .. code-tab:: julia
           :caption: Julia

           using HTTP
           response = HTTP.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/status",
               headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
           )
           status = JSON3.read(String(HTTP.body(response)))

       .. code-tab:: c
           :caption: C/C++

           #include <stdio.h>
           #include <curl/curl.h>

           int main() {
               CURL *curl;
               CURLcode res;

               curl = curl_easy_init();
               if (!curl) {
                   fprintf(stderr, "Failed to initialize curl\n");
                   return 1;
               }

               curl_easy_setopt(curl, CURLOPT_URL,
                   "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/status");

               struct curl_slist *headers = NULL;
               headers = curl_slist_append(headers,
                   "Authorization: Bearer YOUR_ACCESS_TOKEN");
               curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

               res = curl_easy_perform(curl);
               if (res != CURLE_OK) {
                   fprintf(stderr, "curl_easy_perform() failed: %s\n",
                           curl_easy_strerror(res));
               }

               curl_slist_free_all(headers);
               curl_easy_cleanup(curl);

               return 0;
           }

---

.. _databrowser-api-zarr-html:

Retrieving an HTML representation of a Zarr dataset
---------------------------------------------------

.. http:get:: /api/freva-nextgen/data-portal/zarr-utils/html

   This endpoint returns a human-readable HTML representation of a Zarr
   dataset. Internally, the dataset is opened using Xarray and rendered using
   its built-in HTML formatter. The resulting HTML document contains a rich,
   interactive summary that is well-suited for inspection in a web browser.

   This endpoint is intended for interactive exploration only. It does *not*
   return Zarr data directly and should not be used for programmatic access or
   large-scale data processing.

   :reqheader Authorization: Bearer token for authentication.
   :reqheader Content-Type: application/json

   :statuscode 200: HTML representation returned successfully.
   :statuscode 401: Unauthorised / invalid or missing token.
   :statuscode 404: Dataset not found or unknown.
   :statuscode 503: The service is currently unavailable.

   :resheader Content-Type: ``text/html``


   Example Request
   ~~~~~~~~~~~~~~~

   .. sourcecode:: http

       GET /api/freva-nextgen/data-portal/zarr-utils/html HTTP/1.1
       Host: www.freva.dkrz.de
       Authorization: Bearer your_access_token

   Example Response
   ~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 200 OK
       Content-Type: text/html; charset=UTF-8

       <!DOCTYPE html>
       <html>
         <head>
           <meta charset="utf-8">
           <title>Xarray Dataset</title>
         </head>
         <body>
           <!-- HTML rendering of the dataset -->
           <div class="xr-wrap">
               ...
           </div>
         </body>
       </html>

   Example
   ~~~~~~~

   Below you can find example usages of this request in different
   programming languages.

   .. tabs::

       .. code-tab:: bash
           :caption: Shell

           curl -X GET \
             'https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/html' \
             -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

       .. code-tab:: python
           :caption: Python

           import requests

           response = requests.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/html",
               headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"},
           )
           html = response.text
           print(html)

       .. code-tab:: r
           :caption: gnuR

           library(httr)
           response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/html",
               add_headers(Authorization = "Bearer YOUR_ACCESS_TOKEN")
           )
           html <- content(response, as = "text")

       .. code-tab:: julia
           :caption: Julia

           using HTTP
           response = HTTP.get(
               "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/html",
               headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
           )
           html = String(HTTP.body(response))

       .. code-tab:: c
           :caption: C/C++

           #include <stdio.h>
           #include <curl/curl.h>

           int main() {
               CURL *curl;
               CURLcode res;

               curl = curl_easy_init();
               if (!curl) {
                   fprintf(stderr, "Failed to initialize curl\n");
                   return 1;
               }

               curl_easy_setopt(
                   curl, CURLOPT_URL,
                   "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr-utils/html"
               );

               struct curl_slist *headers = NULL;
               headers = curl_slist_append(headers,
                   "Authorization: Bearer YOUR_ACCESS_TOKEN");
               curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

               res = curl_easy_perform(curl);
               if (res != CURLE_OK) {
                   fprintf(stderr, "curl_easy_perform() failed: %s\n",
                           curl_easy_strerror(res));
               }

               curl_slist_free_all(headers);
               curl_easy_cleanup(curl);

               return 0;
           }
