# Changelog

All notable changes to this project will be documented in this file.

## [v2510.1.0]
### Chnaged
The `systemuser` endpoint will not query the systems user database any longer



## [v2510.0.1]
### Fixed
Remove the CORS header from STAC-API, since it's supported on Nginx now


## [v2510.0.0]
### Added
- Option to adjust existing flavours from CLI
- New endpoint to display metadata in zarr format.
- Add update flavour endpoint on the Rest
- Add update flavour as a functionality on the freva-client
- Add zarr view endpoint to respond Xarray-HTML formatted response
- Add logout endpoint to clean the SSO session after use.

### Changed
- Move the client id and secret from search parameter to header
- Fall back to code login flow if device login flow is not available


##[v2509.1.0]
### Changed
- Switched to device login flow for python-lib and cli clients.

## [v2509.0.0]
### Added
- Custom databrowser search flavours for the restAPI and the client
- .well-known endpoint for cleints that need to interact with the OIDC server.
- Possibility to create long lived offline tokens.
- New endpoints for directly streaming data based on file paths.

## [v2508.1.0]
### Added
 - Login timeout option for doing the login.
 - Display better information on how to use port forwarding.

## [v2507.0.0]
### Changed
 - Internal changes.
 - Cli bug fix
 - Fix STAC-API 405 status code on preflight OPTIONS
 - Polish the STAC-API docs
 - Fix an issue regarding get_metadata on user-data
 - Aadd better helper on item assets of STAC
 - Change the freva-client to 2508.0.0

## [v2506.0.1]
### Changed
 - Internal changes.

## [v2506.0.0]
### Changed
 - Moved from password based logins to code based auth flow.

## [v2505.0.0]
### Changed
- Creation of static stac caltalogues anologuous to intake-catalogues.
- Added bmounding boxes to search for geographical regions.

## [v2408.0.0]
### Changed
- Only use open id connect discovery url for authentication.

## [v2403.0.3]

### Changed
- Set the solr request timeout to 30 seconds.

## [v2403.0.2]

### Added
- Added release procedure.

## [v2403.0.1]

### Added
- Initial release of the project.


# Template:
## [Unreleased]

### Added
- New feature X.
- New feature Y.

### Changed
- Improved performance in component A.
- Updated dependency B to version 2.0.0.

### Fixed
- Fixed issue causing application crash on startup.
- Fixed bug preventing users from logging in.
