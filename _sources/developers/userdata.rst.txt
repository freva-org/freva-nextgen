Managing User Data and search Mappings
======================================
This chapter describes how to manage user-provided metadata and customise
search behaviour in the Freva Databrowser API. It explains how users can add
and remove metadata entries for their own datasets, and how they can define
custom mappings that tailor the Databrowser’s search fields to their needs.
Each endpoint is documented with request formats, response structures, and
examples in multiple programming languages.



.. _databrowser-api-userdata:

---

Adding and deleting User Data in Databrowser
---------------------------------------------

.. http:post:: /api/freva-nextgen/databrowser/userdata

   This endpoint allows authenticated users to add metadata about their own
   data to the databrowser. Users provide a list of metadata entries and
   optional facets for indexing and searching their datasets.

   :reqbody user_metadata: A list of metadata entries about the user's data to be added. Each entry must include the required fields: **file**, **variable**, **time**, and **time_frequency**.
   :type user_metadata: list[dict[str, str]]

   :reqbody facets: Optional key-value pairs representing metadata search attributes. These facets are used for indexing and searching the data.
   :type facets: dict[str, Any]

   :reqheader Authorization: Bearer token for authentication.
   :reqheader Content-Type: application/json

   :statuscode 202: Request accepted, returns status message indicating ingestion results.
   :statuscode 422: Invalid request parameters.
   :statuscode 500: Failed to add user data due to a server error.

   Example Request
   ~~~~~~~~~~~~~~~~

   The user must authenticate using a valid access token. The metadata entries and facets are included in the JSON body of the request.

   .. sourcecode:: http

       POST /api/freva-nextgen/databrowser/userdata HTTP/1.1
       Host: www.freva.dkrz.de
       Authorization: Bearer YOUR_ACCESS_TOKEN
       Content-Type: application/json

       {
           "user_metadata": [
               {
                   "file": "/data/file1.nc",
                   "variable": "tas",
                   "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                   "time_frequency": "mon",
                   "additional_info": "Sample data file"
               }
           ],
           "facets": {
               "project": "user-data",
               "product": "new",
               "institute": "globe"
           }
       }

   Example Response (Success)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 202 Accepted
       Content-Type: application/json

       {
           "status": "Your data has been successfully added to the databrowser. (Ingested 5 files into Solr and MongoDB)"
       }

   Example Response (No Files)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 202 Accepted
       Content-Type: application/json

       {
           "status": "No data was added to the databrowser. (No files ingested into Solr and MongoDB)"
       }


   Example
   ~~~~~~~

   Below you can find example usages of this request in different scripting and programming languages.

   .. tabs::

       .. code-tab:: bash
           :caption: Shell

           curl -X POST \
           'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata' \
           -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
           -H "Content-Type: application/json" \
           -d '{
               "user_metadata": [
                   {
                       "file": "/data/file1.nc",
                       "variable": "tas",
                       "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                       "time_frequency": "mon",
                       "additional_info": "Sample data file"
                   }
               ],
               "facets": {
                   "project": "user-data",
                   "product": "new",
                   "institute": "globe"
               }
           }'

       .. code-tab:: python
           :caption: Python

           import requests

           url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers = {
               "Authorization": "Bearer YOUR_ACCESS_TOKEN",
               "Content-Type": "application/json"
           }
           data = {
               "user_metadata": [
                   {
                       "file": "/data/file1.nc",
                       "variable": "tas",
                       "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                       "time_frequency": "mon",
                       "additional_info": "Sample data file"
                   }
               ],
               "facets": {
                   "project": "user-data",
                   "product": "new",
                   "institute": "globe"
               }
           }

           response = requests.post(url, headers=headers, json=data)
           print(response.json())

       .. code-tab:: r
           :caption: R

           library(httr)

           url <- "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")
           body <- list(
               user_metadata = list(
                   list(
                       file = "/data/file1.nc",
                       variable = "tas",
                       time = "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                       time_frequency = "mon",
                       additional_info = "Sample data file"
                   )
               ),
               facets = list(
                   project = "user-data",
                   product = "new",
                   institute = "globe"
               )
           )

           response <- POST(url, add_headers(.headers = headers), body = body, encode = "json")
           content <- content(response, "parsed")
           print(content)

       .. code-tab:: julia
           :caption: Julia

           using HTTP, JSON

           url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers = Dict(
               "Authorization" => "Bearer YOUR_ACCESS_TOKEN",
               "Content-Type" => "application/json"
           )
           body = JSON.json(Dict(
               "user_metadata" => [
                   Dict(
                       "file" => "/data/file1.nc",
                       "variable" => "tas",
                       "time" => "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                       "time_frequency" => "mon",
                       "additional_info" => "Sample data file"
                   )
               ],
               "facets" => Dict(
                   "project" => "user-data",
                   "product" => "new",
                   "institute" => "globe"
               )
           ))

           response = HTTP.request("POST", url, headers = headers, body = body)
           println(String(response.body))

       .. code-tab:: c
           :caption: C/C++

           #include <stdio.h>
           #include <curl/curl.h>

           int main() {
               CURL *curl;
               CURLcode res;

               const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata";
               const char *token = "YOUR_ACCESS_TOKEN";
               const char *json_data = "{"
                   "\"user_metadata\": ["
                       "{"
                           "\"file\": \"/data/file1.nc\","
                           "\"variable\": \"tas\","
                           "\"time\": \"[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]\","
                           "\"time_frequency\": \"mon\","
                           "\"additional_info\": \"Sample data file\""
                       "}"
                   "],"
                   "\"facets\": {"
                       "\"project\": \"user-data\","
                       "\"product\": \"new\","
                       "\"institute\": \"globe\""
                   "}"
               "}";

               // Initialize curl
               curl = curl_easy_init();
               if (curl) {
                   struct curl_slist *headers = NULL;
                   headers = curl_slist_append(headers, "Content-Type: application/json");
                   char auth_header[256];
                   snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
                   headers = curl_slist_append(headers, auth_header);

                   // Set the URL
                   curl_easy_setopt(curl, CURLOPT_URL, url);

                   // Set headers
                   curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                   // Set the HTTP method to POST
                   curl_easy_setopt(curl, CURLOPT_POST, 1L);

                   // Set the JSON data to send
                   curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data);

                   // Perform the request
                   res = curl_easy_perform(curl);
                   if (res != CURLE_OK) {
                       fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
                   }

                   // Clean up
                   curl_slist_free_all(headers);
                   curl_easy_cleanup(curl);
               }
               return 0;
           }

.. http:delete:: /api/freva-nextgen/databrowser/userdata

   This endpoint allows authenticated users to delete their previously indexed data from the databrowser. Users specify search keys to identify the data entries they wish to remove.

   :reqbody search_keys: Search keys (key-value pairs) used to identify the data to delete.
   :type search_keys: dict[str, Any]

   :reqheader Authorization: Bearer token for authentication.
   :reqheader Content-Type: application/json

   :statuscode 202: User data has been deleted successfully.
   :statuscode 500: Failed to delete user data due to a server error.

   Example Request
   ~~~~~~~~~~~~~~~

   The user must authenticate using a valid access token. The search keys are provided in the JSON body of the request to specify which data entries to delete.

   .. sourcecode:: http

       DELETE /api/freva-nextgen/databrowser/userdata HTTP/1.1
       Host: www.freva.dkrz.de
       Authorization: Bearer YOUR_ACCESS_TOKEN
       Content-Type: application/json

       {
           "project": "user-data",
           "product": "new",
           "institute": "globe"
       }

   Example Response
   ~~~~~~~~~~~~~~~~

   .. sourcecode:: http

       HTTP/1.1 202 Accepted
       Content-Type: application/json

       {
           "status": "User data has been deleted successfully"
       }

   Example
   ~~~~~~~

   Below you can find example usages of this request in different scripting and programming languages.

   .. tabs::

       .. code-tab:: bash
           :caption: Shell

           curl -X DELETE \
           'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata' \
           -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
           -H "Content-Type: application/json" \
           -d '{
               "project": "user-data",
               "product": "new",
               "institute": "globe"
           }'

       .. code-tab:: python
           :caption: Python

           import requests

           url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers = {
               "Authorization": "Bearer YOUR_ACCESS_TOKEN",
               "Content-Type": "application/json"
           }
           data = {
               "project": "user-data",
               "product": "new",
               "institute": "globe"
           }

           response = requests.delete(url, headers=headers, json=data)
           print(response.json())

       .. code-tab:: r
           :caption: R

           library(httr)

           url <- "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")
           body <- list(
               project = "user-data",
               product = "new",
               institute = "globe"
           )

           response <- DELETE(url, add_headers(.headers = headers), body = body, encode = "json")
           content <- content(response, "parsed")
           print(content)

       .. code-tab:: julia
           :caption: Julia

           using HTTP, JSON

           url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata"
           headers = Dict(
               "Authorization" => "Bearer YOUR_ACCESS_TOKEN",
               "Content-Type" => "application/json"
           )
           body = JSON.json(Dict(
               "project" => "user-data",
               "product" => "new",
               "institute" => "globe"
           ))

           response = HTTP.request("DELETE", url, headers = headers, body = body)
           println(String(response.body))

       .. code-tab:: c
           :caption: C/C++

           #include <stdio.h>
           #include <curl/curl.h>

           int main() {
               CURL *curl;
               CURLcode res;

               const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/userdata";
               const char *token = "YOUR_ACCESS_TOKEN";
               const char *json_data = "{"
                   "\"project\": \"user-data\","
                   "\"product\": \"new\","
                   "\"institute\": \"globe\""
               "}";

               // Initialize curl
               curl = curl_easy_init();
               if (curl) {
                   struct curl_slist *headers = NULL;
                   headers = curl_slist_append(headers, "Content-Type: application/json");
                   char auth_header[256];
                   snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
                   headers = curl_slist_append(headers, auth_header);

                   // Set the URL
                   curl_easy_setopt(curl, CURLOPT_URL, url);

                   // Set headers
                   curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                   // Set the HTTP method to DELETE
                   curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

                   // Set the JSON data to send
                   curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data);

                   // Perform the request
                   res = curl_easy_perform(curl);
                   if (res != CURLE_OK) {
                       fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
                   }

                   // Clean up
                   curl_slist_free_all(headers);
                   curl_easy_cleanup(curl);
               }
               return 0;
           }

---

.. _databrowser-api-flavours:

Managing Flavours
-------------------------

.. http:get:: /api/freva-nextgen/databrowser/flavours

    This endpoint allows you to retrieve all available flavours to the current user.
    Flavours define Data Reference Syntax (DRS) standards and their metadata field mappings.
    The endpoint returns both global(accessible to all) flavours and user-specific custom flavours.

    :query flavour_name: Filter by specific flavour type.
    :type flavour_name: str
    :query owner: Filter by owner ('global' or username).
    :type owner: str
    :reqheader Authorization: Bearer token for authentication (optional).

    :statuscode 200: no error
    :resheader Content-Type: ``application/json``: List of available flavours and their mappings.

                             - ``total``: Total number of flavours found.
                             - ``flavours``: Array of flavour objects containing name, mapping, owner, and creation date.

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/databrowser/flavours?owner=global HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "total": 6,
            "flavours": [
                {
                    "flavour_name": "freva",
                    "mapping": {
                        "project": "project",
                        "product": "product",
                        "institute": "institute",
                        "model": "model",
                        "experiment": "experiment"
                    },
                    "owner": "global",
                    "created_at": "2024-01-01T00:00:00"
                },
                {
                    "flavour_name": "cmip6",
                    "mapping": {
                        "experiment": "experiment_id",
                        "ensemble": "member_id",
                        "institute": "institution_id",
                        "model": "source_id"
                    },
                    "owner": "global",
                    "created_at": "2024-01-01T00:00:00"
                }
            ]
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET \
            'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours?owner=global' \
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

        .. code-tab:: python
            :caption: Python

            import requests

            headers = {"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
            params = {"owner": "global"}

            response = requests.get(
                "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours",
                headers=headers,
                params=params
            )
            data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")
            response <- GET(
                "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours",
                add_headers(.headers = headers),
                query = list(owner = "global")
            )
            data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

        .. code-tab:: julia
            :caption: Julia

            using HTTP, JSON

            headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
            response = HTTP.get(
                "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours",
                headers = headers,
                query = Dict("owner" => "global")
            )
            data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl = curl_easy_init();
                if (curl) {
                    const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours?owner=global";

                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, url);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                    res = curl_easy_perform(curl);

                    curl_slist_free_all(headers);
                    curl_easy_cleanup(curl);
                }
                return 0;
            }

.. http:post:: /api/freva-nextgen/databrowser/flavours

    This endpoint allows authenticated users to add a new custom flavour definition.
    Flavours define how metadata field names are mapped between different DRS standards.
    Admin users can create ``global`` flavours, while regular users can create personal flavours.

    :reqbody flavour_name: The name of the new flavour.
    :type flavour_name: str
    :reqbody mapping: Dictionary mapping freva standard field names to the new flavour's field names.
    :type mapping: dict[str, str]
    :reqbody is_global: Whether this should be a global flavour (admin only).
    :type is_global: bool
    :reqheader Authorization: Bearer token for authentication.
    :reqheader Content-Type: application/json

    :statuscode 201: Flavour created successfully.
    :statuscode 401: Unauthorized / not a valid token.
    :statuscode 403: Forbidden - only admin users can create global flavours.
    :statuscode 409: Conflict - flavour already exists.
    :statuscode 422: Invalid flavour definition.
    :statuscode 500: Internal server error - failed to add flavour.

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/databrowser/flavours HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN
        Content-Type: application/json

        {
            "flavour_name": "my_custom_drs",
            "mapping": {
                "project": "proj_id",
                "product": "prod_name",
                "institute": "institution",
                "model": "model_name",
                "experiment": "exp_id",
                "variable": "var_name"
            },
            "is_global": false
        }

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "status": "Flavour 'my_custom_drs' added successfully"
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X POST \
            'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours' \
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
            -H "Content-Type: application/json" \
            -d '{
                "flavour_name": "my_custom_drs",
                "mapping": {
                    "project": "proj_id",
                    "product": "prod_name",
                    "institute": "institution",
                    "model": "model_name",
                    "experiment": "exp_id",
                    "variable": "var_name"
                },
                "is_global": false
            }'

        .. code-tab:: python
            :caption: Python

            import requests

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours"
            headers = {
                "Authorization": "Bearer YOUR_ACCESS_TOKEN",
                "Content-Type": "application/json"
            }
            data = {
                "flavour_name": "my_custom_drs",
                "mapping": {
                    "project": "proj_id",
                    "product": "prod_name",
                    "institute": "institution",
                    "model": "model_name",
                    "experiment": "exp_id",
                    "variable": "var_name"
                },
                "is_global": False
            }

            response = requests.post(url, headers=headers, json=data)
            print(response.json())

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            url <- "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours"
            headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")
            body <- list(
                flavour_name = "my_custom_drs",
                mapping = list(
                    project = "proj_id",
                    product = "prod_name",
                    institute = "institution",
                    model = "model_name",
                    experiment = "exp_id",
                    variable = "var_name"
                ),
                is_global = FALSE
            )

            response <- POST(url, add_headers(.headers = headers), body = body, encode = "json")
            content <- content(response, "parsed")
            print(content)

        .. code-tab:: julia
            :caption: Julia

            using HTTP, JSON

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours"
            headers = Dict(
                "Authorization" => "Bearer YOUR_ACCESS_TOKEN",
                "Content-Type" => "application/json"
            )
            body = JSON.json(Dict(
                "flavour_name" => "my_custom_drs",
                "mapping" => Dict(
                    "project" => "proj_id",
                    "product" => "prod_name",
                    "institute" => "institution",
                    "model" => "model_name",
                    "experiment" => "exp_id",
                    "variable" => "var_name"
                ),
                "is_global" => false
            ))

            response = HTTP.request("POST", url, headers = headers, body = body)
            println(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours";
                const char *json_data = "{"
                    "\"flavour_name\": \"my_custom_drs\","
                    "\"mapping\": {"
                        "\"project\": \"proj_id\","
                        "\"product\": \"prod_name\","
                        "\"institute\": \"institution\","
                        "\"model\": \"model_name\","
                        "\"experiment\": \"exp_id\","
                        "\"variable\": \"var_name\""
                    "},"
                    "\"is_global\": false"
                "}";

                curl = curl_easy_init();
                if (curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Content-Type: application/json");
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, url);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                    curl_easy_setopt(curl, CURLOPT_POST, 1L);
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data);

                    res = curl_easy_perform(curl);

                    curl_slist_free_all(headers);
                    curl_easy_cleanup(curl);
                }
                return 0;
            }

.. http:put:: /api/freva-nextgen/databrowser/flavours/(str:flavour_name)

    This endpoint allows authenticated users to update an existing custom flavour definition.
    You can update the mapping (partial or complete) and optionally rename the flavour by providing
    a different ``flavour_name`` in the request body. Admin users can update global flavours,
    while regular users can only update their own personal flavours.

    :param flavour_name: The name of the flavour to update.
    :type flavour_name: str
    :reqbody flavour_name: The name for the flavour (can be same as current or new name for renaming).
    :type flavour_name: str
    :reqbody mapping: Partial or complete dictionary mapping freva standard field names to update.
                      Only provided keys will be updated; other mappings remain unchanged.
    :type mapping: dict[str, str]
    :reqbody is_global: Whether this is a global flavour (admin only).
    :type is_global: bool
    :reqheader Authorization: Bearer token for authentication.
    :reqheader Content-Type: application/json

    :statuscode 200: Flavour updated successfully.
    :statuscode 401: Unauthorized / not a valid token.
    :statuscode 403: Forbidden - only admin users can update global flavours.
    :statuscode 404: Flavour not found.
    :statuscode 409: Conflict - new flavour name already exists.
    :statuscode 422: Invalid flavour definition.
    :statuscode 500: Internal server error - failed to update flavour.

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        PUT /api/freva-nextgen/databrowser/flavours/my_custom_drs HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN
        Content-Type: application/json

        {
            "flavour_name": "my_custom_drs",
            "mapping": {
                "model": "updated_model_name",
                "experiment": "updated_exp_id"
            },
            "is_global": false
        }

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "status": "Flavour updated successfully"
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X PUT \
            'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs' \
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
            -H "Content-Type: application/json" \
            -d '{
                "flavour_name": "my_custom_drs",
                "mapping": {
                    "model": "updated_model_name",
                    "experiment": "updated_exp_id"
                },
                "is_global": false
            }'

        .. code-tab:: python
            :caption: Python

            import requests

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers = {
                "Authorization": "Bearer YOUR_ACCESS_TOKEN",
                "Content-Type": "application/json"
            }
            data = {
                "flavour_name": "my_custom_drs",
                "mapping": {
                    "model": "updated_model_name",
                    "experiment": "updated_exp_id"
                },
                "is_global": False
            }

            response = requests.put(url, headers=headers, json=data)
            print(response.json())

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            url <- "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")
            body <- list(
                flavour_name = "my_custom_drs",
                mapping = list(
                    model = "updated_model_name",
                    experiment = "updated_exp_id"
                ),
                is_global = FALSE
            )

            response <- PUT(url, add_headers(.headers = headers), body = body, encode = "json")
            content <- content(response, "parsed")
            print(content)

        .. code-tab:: julia
            :caption: Julia

            using HTTP, JSON

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers = Dict(
                "Authorization" => "Bearer YOUR_ACCESS_TOKEN",
                "Content-Type" => "application/json"
            )
            body = JSON.json(Dict(
                "flavour_name" => "my_custom_drs",
                "mapping" => Dict(
                    "model" => "updated_model_name",
                    "experiment" => "updated_exp_id"
                ),
                "is_global" => false
            ))

            response = HTTP.request("PUT", url, headers = headers, body = body)
            println(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs";
                const char *json_data = "{"
                    "\"flavour_name\": \"my_custom_drs\","
                    "\"mapping\": {"
                        "\"model\": \"updated_model_name\","
                        "\"experiment\": \"updated_exp_id\""
                    "},"
                    "\"is_global\": false"
                "}";

                curl = curl_easy_init();
                if (curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Content-Type: application/json");
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, url);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data);

                    res = curl_easy_perform(curl);

                    curl_slist_free_all(headers);
                    curl_easy_cleanup(curl);
                }
                return 0;
            }

.. http:delete:: /api/freva-nextgen/databrowser/flavours/(str:flavour_name)

    This endpoint allows authenticated users to delete a custom flavour definition.
    Admin users can delete global flavours, while regular users can only delete their own personal flavours.
    Built-in flavours such as ``cmip5``, ``cmip6``, ``cordex``, ``freva`` and ``user``, cannot be deleted nor
    by regular users nor by admins.

    :param flavour_name: The name of the flavour to delete.
    :type flavour_name: str
    :reqheader Authorization: Bearer token for authentication.

    :statuscode 200: Flavour deleted successfully.
    :statuscode 401: Unauthorized / not a valid token.
    :statuscode 404: Flavour not found.
    :statuscode 422: Cannot delete built-in flavour.
    :statuscode 500: Internal server error - failed to delete flavour.

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        DELETE /api/freva-nextgen/databrowser/flavours/my_custom_drs HTTP/1.1
        Host: www.freva.dkrz.de
        Authorization: Bearer YOUR_ACCESS_TOKEN

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "status": "Personal flavour 'my_custom_drs' deleted successfully"
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages.

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X DELETE \
            'https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs' \
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

        .. code-tab:: python
            :caption: Python

            import requests

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers = {"Authorization": "Bearer YOUR_ACCESS_TOKEN"}

            response = requests.delete(url, headers=headers)
            print(response.json())

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            url <- "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers <- c(Authorization = "Bearer YOUR_ACCESS_TOKEN")

            response <- DELETE(url, add_headers(.headers = headers))
            content <- content(response, "parsed")
            print(content)

        .. code-tab:: julia
            :caption: Julia

            using HTTP, JSON

            url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs"
            headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")

            response = HTTP.request("DELETE", url, headers = headers)
            println(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl = curl_easy_init();
                if (curl) {
                    const char *url = "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/flavours/my_custom_drs";

                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, url);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

                    res = curl_easy_perform(curl);

                    curl_slist_free_all(headers);
                    curl_easy_cleanup(curl);
                }
                return 0;
            }

---


.. note::
   Please note that in these examples,
   "https://www.freva.dkrz.de" were used as a placeholder URL.
   You should replace it with the actual URL of your
   Freva Databrowser REST API. The responses above are truncated for brevity.
   The actual response will include more datasets in the `catalog_dict` list.
