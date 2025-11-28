The restAPI auth endpoints
==========================
The API supports token-based authentication using OAuth2. To obtain an access
token, clients can use the ``/api/freva-nextgen/auth/v2/token`` endpoint by
providing valid username and password credentials. The access token should
then be included in the authorization header for secured endpoints.

.. http:post:: /api/freva-nextgen/auth/v2/token

    Create an new login token from a username and password.
    You should either set a username and password or an existing refresh token.
    You can also set the client_id. Client id's are configured to gain access,
    specific access for certain users. If you don't set the client_id, the
    default id will be chosen.

    :form code:          The code received as part of the OAuth2 authorization
                         code flow
    :type code:          str
    :form redirect_uri:  The URI to which the authorization server will redirect
                         the user after authentication. It must match one of the
                         URIs registered with the OAuth2 provider
    :type redirect_uri: str
    :form refresh-token: The refresh token that is used to create a new token
                         the refresh token can be used instead of authorizing
                         via user creentials.
    :type refresh-token: str
    :form client_id: The unique identifier for your application used to
                     request an OAuth2 access token from the authentication
                     server, this form parameter is optional.
    :statuscode 200: no error
    :statuscode 401: unauthorized
    :resheader Content-Type: ``application/json``: access and refresh token.


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/auth/v2/token HTTP/1.1
        host: www.freva.dkrz.de

        {
            "refresh-token": "my-token",
        }

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6.."
            "token_type": "Bearer",
            "expires": 1722874908,
            "refresh_token": "eyJhbGciOiJIUzUxMiIsInR5cCIgOiAiSldUIiwia2lkIi.."
            "refresh_expires": 1722876408,
            "scope": "profile email address",
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X POST https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/token \
             -d "username=janedoe" \
             -d "password=janedoe123"

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.post(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/token",
              data={"refresh-token": "mytoken"}
            )
            token_data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            url <- "https://freva.dkrz.de/api/freva-nextgen/auth/v2/token"
            response <- POST(
               "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/token",
               body = setNames(list("mytoken"), "refresh-token"),
               encode = "form"
            )
            token_data <- content(response, "parsed")

        .. code-tab:: julia
            :caption: Julia

            using HTTP
            using JSON

            response = HTTP.POST(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/token",
              body = Dict("refresh-token" => "mytoken")
            )
            token_data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");

                    curl_easy_setopt(curl, CURLOPT_URL, "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/token");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "refresh-token=mytoken");

                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }


---


.. http:get:: /api/freva-nextgen/.well-known/openid-configuration

    Get configuration information about the identity provider in use.


    :statuscode 200: Metadata for interacting with the OIDC provider.
    :statuscode 503: OIDC Identity Provider server unavailable.
    :resheader Content-Type: ``application/json``:  Metadata for interacting with
                                                    the OIDC provider.


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/.well-known/openid-configuration HTTP/1.1
        host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "issuer": "http://localhost:8080/realms/freva",
            "authorization_endpoint": "http://localhost:8080/realms/freva/protocol/openid-connect/auth",
            "token_endpoint": "http://localhost:8080/realms/freva/protocol/openid-connect/token"
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.drkz.de/api/freva-nextgen/.well-known/openid-configuration

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
              "https://www.freva.drkz.de/api/freva-nextgen/.well-known/openid-configuration",
            )
            token_data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            response <- GET(
               "https://www.freva.drkz.de/api/freva-nextgen/.well-known/openid-configuration"
            )
            token_data <- content(response, "parsed")

        .. code-tab:: julia
            :caption: Julia

            using HTTP
            using JSON

            response = HTTP.get(
              "https://www.freva.drkz.de/api/freva-nextgen/.well-known/openid-configuration"
            )
            token_data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    struct curl_slist *headers = NULL;

                    curl_easy_setopt(curl, CURLOPT_URL, "https://www.freva.drkz.de/api/freva-nextgen/.well-known/openid-configuration");
                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }

---

.. http:get:: /api/freva-nextgen/auth/v2/status

    Check the status of an access token.



    :reqheader Authorization: The OAuth2 access token
    :statuscode 200: no error
    :statuscode 401: unauthorized
    :resheader Content-Type: ``application/json``: access and refresh token.


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/auth/v2/status HTTP/1.1
        host: www.freva.dkrz.de
        Authorization: Bearer your_access_token

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "sub": "648692af-aaed-4f82-9f74-2d6baf96f5ea",
            "exp": 1719261824,
            "email": "jane@example.com"
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/status \
             -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/status",
              headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
            )
            token_data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/status",
               add_headers(Authorization = paste("Bearer", "YOUR_ACCESS_TOKEN"))
            )
            token_data <- content(response, "parsed")

        .. code-tab:: julia
            :caption: Julia

            using HTTP
            using JSON

            response = HTTP.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/status",
              headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
            )
            token_data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/status");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }

---


.. http:get:: /api/freva-nextgen/auth/v2/userinfo

    Get userinfo for the current token.


    :reqheader Authorization: The OAuth2 access token
    :statuscode 200: no error
    :statuscode 401: unauthorized
    :resheader Content-Type: ``application/json``: access and refresh token.


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/auth/v2/userinfo HTTP/1.1
        host: www.freva.dkrz.de
        Authorization: Bearer your_access_token

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "username": "janedoe",
            "first_name": "Jane",
            "last_name": "Doe",
            "email": "jane@example.com"
            "home": ""
            "is_guest": true
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/userinfo \
             -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/userinfo",
              headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
            )
            token_data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/userinfo",
               add_headers(Authorization = paste("Bearer", "YOUR_ACCESS_TOKEN"))
            )
            token_data <- content(response, "parsed")

        .. code-tab:: julia
            :caption: Julia

            using HTTP
            using JSON

            response = HTTP.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/userinfo",
              headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
            )
            token_data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/userinfo");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }

---


.. http:get:: /api/freva-nextgen/auth/v2/checkuser

    Check if user token is authorized.

    This endpoint can be useful to check if a user token can be used to
    perform or access restricted actions and resources.


    :reqheader Authorization: The OAuth2 access token
    :statuscode 200: no error
    :statuscode 401: unauthorized
    :resheader Content-Type: ``application/json``: access and refresh token.


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/auth/v2/checkuser HTTP/1.1
        host: www.freva.dkrz.de
        Authorization: Bearer your_access_token

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
           "pw_name": "janedoe
        }

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/checkuser \
             -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/checkuser",
              headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
            )
            token_data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/checkuser",
               add_headers(Authorization = paste("Bearer", "YOUR_ACCESS_TOKEN"))
            )
            token_data <- content(response, "parsed")

        .. code-tab:: julia
            :caption: Julia

            using HTTP
            using JSON

            response = HTTP.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/checkuser",
              headers = Dict("Authorization" => "Bearer YOUR_ACCESS_TOKEN")
            )
            token_data = JSON.parse(String(response.body))

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "Authorization: Bearer YOUR_ACCESS_TOKEN");

                    curl_easy_setopt(curl, CURLOPT_URL, "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/checkuser");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }

---


.. http:get:: /api/freva-nextgen/auth/v2/logout

    Logout endpoint that redirects to the identity provider's logout page.
    This endpoint terminates the user's session and optionally redirects to a
    specified URL after logout completes.

    :query post_logout_redirect_uri: Optional URL to redirect after logout completes.
                                      Must be registered with the OIDC provider.
    :type post_logout_redirect_uri: str
    :statuscode 307: Redirect to IDP logout endpoint
    :statuscode 400: Invalid post_logout_redirect_uri


    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/auth/v2/logout?post_logout_redirect_uri=https://www.freva.dkrz.de/ HTTP/1.1
        host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 307 Temporary Redirect
        Location: https://idp-example.com/realms/freva/protocol/openid-connect/logout?client_id=freva-client&post_logout_redirect_uri=https://freva.dkrz.com/

    Code examples
    ~~~~~~~~~~~~~
    Below you can find example usages of this request in different scripting and
    programming languages

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/logout?post_logout_redirect_uri=https://www.freva.dkrz.de/"

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/logout",
              params={"post_logout_redirect_uri": "https://www.freva.dkrz.de/"},
              allow_redirects=True
            )

        .. code-tab:: r
            :caption: gnuR

            library(httr)

            response <- GET(
               "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/logout",
               query = list(post_logout_redirect_uri = "https://www.freva.dkrz.de/")
            )

        .. code-tab:: julia
            :caption: Julia

            using HTTP

            response = HTTP.get(
              "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/logout",
              query = Dict("post_logout_redirect_uri" => "https://www.freva.dkrz.de/")
            )

        .. code-tab:: c
            :caption: C/C++

            #include <stdio.h>
            #include <curl/curl.h>

            int main() {
                CURL *curl;
                CURLcode res;

                curl_global_init(CURL_GLOBAL_DEFAULT);
                curl = curl_easy_init();
                if(curl) {
                    curl_easy_setopt(curl, CURLOPT_URL,
                        "https://www.freva.dkrz.de/api/freva-nextgen/auth/v2/logout?post_logout_redirect_uri=https://www.freva.dkrz.de/");
                    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

                    res = curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                }
                curl_global_cleanup();
                return 0;
            }

---


Notes on Code-Based Auth Flow
-----------------------------

Code-based authentication is the only supported method since
version ``2505.1.0``. It follows the OAuth2 Authorization Code Flow and is
suitable for both end users and Service Provider (SP) integrations.
However, there are important guidelines and limitations you should be aware of.

🔒 In most cases: Do not call ``/login`` or ``/callback`` directly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The endpoints ``/auth/v2/login`` and ``/auth/v2/callback`` are internal
coordination points for the authentication process:

- ``/login`` initiates the code flow by redirecting to the OpenID Connect provider.
- ``/callback`` is the endpoint where the OpenID provider sends the authorization code.

These endpoints are **not designed for direct use by end users** or typical API consumers.
Calling them directly will likely lead to errors or unexpected behaviour.

Instead, you should authenticate using **one of the supported client tools**:

- The **Python client**: via :py:func:`freva_client.authenticate`
- The **CLI tool**: via :ref:`freva-client auth <auth_cli>`
- The **web portal**: to manually log in and download a token file

These interfaces abstract away the complexity and ensure the flow is handled securely and correctly.

For Service Providers (SPs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are building a custom SP (e.g. a web service or interactive tool
that needs to act on behalf of a user), then it is appropriate to
interact with the code flow endpoints directly — but only with care.

Follow these steps:

1. **Redirect the user to** Freva’s ``/login`` endpoint:

   .. sourcecode:: http

      GET /api/freva-nextgen/auth/v2/login?redirect_uri=https://your-sp.com/callback?offline_access=true HTTP/1.1
      host: www.freva.dkrz.de

   - The ``redirect_uri`` must be a publicly accessible endpoint on your service that handles the code exchange.
   - The ``offline_access`` parameter can be used to request an onffline token with a long TTL.

2. **User authenticates** via the upstream Identity Provider (e.g., Keycloak).
3. **The Identity Provider redirects back** to your service's ``/callback`` endpoint with a `code` and `state` query parameter.

4. **Your SP must then POST the code to** Freva's token exchange endpoint:

   .. sourcecode:: http

      POST /api/freva-nextgen/auth/v2/token HTTP/1.1
      host: www.freva.dkrz.de
      Content-Type: application/x-www-form-urlencoded

      {
          code=XXX&redirect_uri=https://your-sp.com/callback
      }

   - This will return a JSON with access token, refresh token, and expiry info.

5. **Use the access token** to authenticate future API requests.
6. **Optionally refresh the token** before expiry using:

   .. sourcecode:: http

      POST /api/freva-nextgen/auth/v2/token HTTP/1.1
      host: www.freva.dkrz.de
      Content-Type: application/x-www-form-urlencoded

      {
            grant_type=refresh_token&refresh_token=YYY
      }

.. note::

   - You do **not need** to manage client secrets for browser-based SPs.
   - The ``redirect_uri`` must match one of the values registered with the OIDC provider.
   - If you are building a backend-for-frontend (BFF) architecture, handle the
     token exchange **server-side** to protect credentials.

This is the recommended approach for implementing a standards-compliant
OAuth2 Authorization Code Flow as a Service Provider. A detailed example
is given in the :ref:`auth_example` section.
