Zarr Streaming API
==================
Definition of endpoints for loading/streaming and manipulating data.

This API exposes asynchronous services that convert files or objects into Zarr
stores and stream them to clients.  The conversion is performed by a
background worker and communicated via a message broker; the REST endpoints
return references (URLs) to the resulting Zarr datasets rather than the data
itself.

.. _zarr_convert:

Request asynchronous Zarr conversion
------------------------------------

.. http:get:: /api/freva-nextgen/data-portal/zarr/convert

   Submit one or more file or object paths to be converted into Zarr stores.
   This endpoint only publishes a message to the data‑portal worker via a broker;
   it does **not** verify that the paths exist or perform the conversion itself.
   It returns a JSON object with a ``urls`` array; each entry contains a UUID
   that identifies the future Zarr dataset.

   If the data‑loading service cannot access a file, it will record the failure
   and the corresponding Zarr dataset will be in a failed state with a reason.
   You can query the status endpoint to check whether the conversion succeeded
   or failed.

   :query path: Absolute or object‑store paths to the input files.  Repeat this
                parameter to submit multiple files.
   :type path: ``str`` | ``list[str]``
   :reqheader Authorization: Bearer token for authentication.
   :statuscode 200: JSON object with a ``urls`` array containing the Zarr endpoint URLs.
   :statuscode 401: The user could not be authenticated.
   :statuscode 503: Service is currently unavailable.
   :statuscode 500: Internal error while publishing the data request.
   :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/data-portal/zarr/convert?path=/work/abc123/myuser/mydata_1.nc&path=/work/abc123/myuser/mydata_2.nc HTTP/1.1
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

    Code examples
    ~~~~~~~~~~~~~

    Below are example usages of this request in different languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            # Use curl with --get (-G) and multiple --data-urlencode parameters
            curl -G \
              'https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert' \
              --header "Authorization: Bearer YOUR_ACCESS_TOKEN" \
              --data-urlencode 'path=/work/abc123/myuser/mydata_1.nc' \
              --data-urlencode 'path=/work/abc123/myuser/mydata_2.nc'

        .. code-tab:: python
            :caption: Python (requests)

            import requests

            response = requests.get(
                "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert",
                params={
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

            response <- GET(
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
            response = HTTP.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/data-portal/zarr/convert";
              headers = headers,
              query = query,
            )
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
