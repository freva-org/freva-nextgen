.. _auth:

Authentication
==============

Some functionality of the freva-rest API and the client library is only
accessible after successful authentication. This authentication is realised
with OAuth2 token creation. You can create new access and refresh tokens.
Refresh tokens can be used to create new access tokens without needing to log
in via username and password, thereby minimising the risk of exposing login
credentials. Bear in mind that both login and refresh tokens have a limited
lifetime.

Generally speaking, you have three options to interact with the authorization
system:


- via the REST API ``/api/freva-nextgen/auth/v2`` endpoints
- via the :py:func:`freva_client.authenticate` function
- via the ``freva-client auth`` command-line interface


.. warning::

   Starting with version 2506.0.0, the **password grant type is no longer supported**.

   Authentication must now be performed using the **authorization code flow**.
   Unless you want to setup a service provider, we advice against using the
   restAPI endpoints for authentication.


.. toctree::
   :maxdepth: 1
   :caption: RestAPI Guide

   endpoints
   app_example
