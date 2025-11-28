.. _auth_example:

Integrating Authentication into your Web Applications
----------------------------------------------------
Freva supports OAuth2-based authentication using the Authorization Code Flow.

You can use the authentication endpoints of Freva's RestAPI to secure your
web applications with OpenID Connect (OIDC) authentication flow and creating
OAuth2 tokens.



Authentication Flow
+++++++++++++++++++

The standard OIDC Authorization Code flow involves three steps:

1. Redirect to Login
~~~~~~~~~~~~~~~~~~~~

Redirect the user to the login endpoint with the `redirect_uri` parameter.

.. sourcecode:: http

   GET /api/freva-nextgen/auth/v2/login HTTP/1.1
   host: www.freva.dkrz.de

   {
        redirect_uri=http://localhost:8050/callback
   }

2. Authorization Callback
~~~~~~~~~~~~~~~~~~~~~~~~~

After user login, the identity provider redirects to your specified `redirect_uri`
with the following query parameters:

- ``code``: a temporary authorization code
- ``state``: an opaque anti-CSRF token

You must exchange this ``code`` value with the Freva `/token` endpoint.

.. sourcecode:: http

   POST /api/freva-nextgen/auth/v2/token HTTP/1.1
   host: www.freva.dkrz.de
   Content-Type: application/x-www-form-urlencoded

   {
        code=xyz122&redirect_uri=http://localhost:8050/callback&grant_type=authorization_code
   }

This returns an OAuth2 access token and optional refresh token.

3. Retrieve User Info (optional):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the generated access token to fetch the user's identity:

.. sourcecode:: http

   GET /api/freva-nextgen/auth/v2/userinfo HTTP/1.1
   host: www.freva.dkrz.de
   Authorization: Bearer access_token

Alternatively, you can use the ``/systemuser`` endpoint to retrieve more detailed i
information about the authenticated user. This endpoint is only accessible
to primary (non-guest) users, making it useful for enforcing access
restrictions or `authorization` in your application.
By calling ``/systemuser`` with a valid access token, you can reliably verify
whether the current user is a full (primary) account holder.

.. sourcecode:: http

   GET /api/freva-nextgen/auth/v2/systemuser HTTP/1.1
   host: www.freva.dkrz.de
   Authorization: Bearer access_token


Dash Example
++++++++++++

The following `Plotly Dash <https://dash.plotly.com/introduction>`_
app demonstrates how to integrate this authentication flow using Flask sessions
to store tokens.
It uses `requests` for API interaction and displays a simple authenticated
Plotly graph.

.. literalinclude:: dash_auth_example.py
   :language: python
   :linenos:

You can download the example file here: :download:`dash_auth_example.py <dash_auth_example.py>`.

**Security Notes**

- *Redirect URI*: Must match the URI registered with the identity provider.
                    Contact your friendly freva admins for advice.
- *Session Secret*: Set ``server.secret_key`` using a secure environment variable in production.
- *HTTPS*: Always use HTTPS in production to protect token integrity.
- Use a WSGI-compatible server like Gunicorn for deployment:

  .. code-block:: console

    gunicorn dash_auth_example:server --bind 0.0.0.0:8050

.. seealso::

    - `OpenID Connect Core Specification <https://openid.net/specs/openid-connect-core-1_0.html>`_
    - `OAuth 2.0 Authorization Framework <https://datatracker.ietf.org/doc/html/rfc6749>`_
