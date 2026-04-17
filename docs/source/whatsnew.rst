Changelog
~~~~~~~~~

All notable changes to this project will be documented in this file.

v2604.0.0
^^^^^^^^^

Changed
"""""""
- Add token mint functionality to issue auth-tokens and be compliant with OAuth2

v2603.1.1
^^^^^^^^^

Fix
"""
- Ditch fastapi-third-party-auth dependency entirely from Freva-rest.

v2603.1.0
^^^^^^^^^

Changed
"""""""
- Migrate restAPI auth library from fastapi-third-party-auth to py-oidc-auth[fastapi].
- Remove `systemuser` endpoint from the rest-api

v2603.0.0
^^^^^^^^^

Changed
"""""""
- Migrate freva-client auth library to py-oidc-auth-client.

v2601.0.0
^^^^^^^^^

Added
"""""
- Faster redis connections.
- Better pre-signed public zarr-store urls.
- Lazy import of 'slow' dependencies.
- Fix issues #65 - recycling redis connections.
- Enabled PKCE for login flow.

v2511.0.0
^^^^^^^^^

Added
"""""
- Support for creating public pre-signed zarr stores.
- On-demand loading for zarr stores.

v2510.1.2
^^^^^^^^^

Fixed
"""""
- Bug fixing.

v2510.1.1
^^^^^^^^^

Fixed
"""""
- Redis connection bug.

v2510.1.0
^^^^^^^^^

Added
"""""
- Add support for remote data on data loader.

Changed
"""""""
- The ``systemuser`` endpoint will not query the systems user database any longer.

v2510.0.1
^^^^^^^^^

Fixed
"""""
- Remove the CORS header from STAC-API, since it's supported on Nginx now.

v2510.0.0
^^^^^^^^^

Added
"""""
- Option to adjust existing flavours from CLI.
- New endpoint to display metadata in zarr format.
- Add update flavour endpoint on the Rest.
- Add update flavour as a functionality on the freva-client.
- Add zarr view endpoint to respond Xarray-HTML formatted response.
- Add logout endpoint to clean the SSO session after use.

Changed
"""""""
- Move the client id and secret from search parameter to header.
- Fall back to code login flow if device login flow is not available.

v2509.1.0
^^^^^^^^^

Changed
"""""""
- Switched to device login flow for python-lib and cli clients.

v2509.0.0
^^^^^^^^^

Added
"""""
- Custom databrowser search flavours for the restAPI and the client.
- ``.well-known`` endpoint for clients that need to interact with the OIDC server.
- Possibility to create long lived offline tokens.
- New endpoints for directly streaming data based on file paths.

v2508.1.0
^^^^^^^^^

Added
"""""
- Login timeout option for doing the login.
- Display better information on how to use port forwarding.

v2507.0.0
^^^^^^^^^

Changed
"""""""
- Internal changes.
- CLI bug fix.
- Fix STAC-API 405 status code on preflight OPTIONS.
- Polish the STAC-API docs.
- Fix an issue regarding get_metadata on user-data.
- Add better helper on item assets of STAC.
- Change the freva-client to 2508.0.0.

v2506.0.1
^^^^^^^^^

Changed
"""""""
- Internal changes.

v2506.0.0
^^^^^^^^^

Changed
"""""""
- Moved from password based logins to code based auth flow.

v2505.0.0
^^^^^^^^^

Changed
"""""""
- Creation of static STAC catalogues analogous to intake-catalogues.
- Added bounding boxes to search for geographical regions.

v2408.0.0
^^^^^^^^^

Changed
"""""""
- Only use OpenID Connect discovery URL for authentication.

v2403.0.3
^^^^^^^^^

Changed
"""""""
- Set the Solr request timeout to 30 seconds.

v2403.0.2
^^^^^^^^^

Added
"""""
- Added release procedure.

v2403.0.1
^^^^^^^^^

Added
"""""
- Initial release of the project.
