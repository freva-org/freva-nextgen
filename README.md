# A REST API for the freva databrowser

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-red.svg)](https://www.python.org/downloads/release/python-312/)
[![Docs](https://img.shields.io/badge/API-Doc-green.svg)](https://freva-clint.github.io/databrowserAPI)
[![Tests](https://github.com/FREVA-CLINT/databrowserAPI/actions/workflows/ci_job.yml/badge.svg)](https://github.com/FREVA-CLINT/databrowserAPI/actions)
[![Test-Coverage](https://codecov.io/github/FREVA-CLINT/databrowserAPI/branch/init/graph/badge.svg?token=dGhXxh7uP3)](https://codecov.io/github/FREVA-CLINT/databrowserAPI)

The Freva Databrowser REST API is a powerful tool that enables you to search
for climate and environmental datasets seamlessly in various programming
languages. By generating RESTful requests, you can effortlessly access
collections of various datasets, making it an ideal resource for climate
scientists, researchers, and data enthusiasts.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Development Environment](#development-environment)
- [Testing](#testing)
- [License](#license)

## Installation

1. Make sure you have Python 3.11+ installed.
2. Clone this repository:

```console
git clone git@github.com:FREVA-CLINT/databrowserAPI.git
cd databrowserAPI
```

4. Install the API:

```console
python -m pip install -e .[dev]
```

## Development Environment
Apache solr is needed run the system in a development environment, here we
set up solr in a docker container using the `docker-compose` command, ensure
you have Docker Compose installed on your system.
Then, run the following command:

```console
docker-compose -f dev-env/docker-compose.yaml up -d --remove-orphans
```
This will start the required services and containers to create the development
environment. You can now develop and test the project within this environment.

After solr is up and running you can start the REST server the following:

```console
python -m databrowser.cli --config-file api_config.toml --debug --dev --port 7777
```

The ``--debug`` and ``--dev`` flag will make sure that any changes are loaded.
You can choose any port you like. Furthermore the ``--dev`` flag will pre
load an empty apache solr server with some data. If you don't like that
simply do not pass the ``--dev`` flag.

### Testing

Unit tests, Example notebook tests, type annotations and code style tests
are done with [tox](https://tox.wiki/en/latest/). To run all tests, linting
in parallel simply execute the following command:

```console
tox -p 3
```
You can also run the each part alone, for example to only check the code style:

```console
tox -e lint
```
available options are ``lint``, ``types``, ``test``.

Tox runs in a separate python environment to run the tests in the current
environment use:

```console
pytest
```
### Creating a new release.

Once the development is finished and you decide that it's time for a new
release of the software use the following command to trigger a release
procedure:

```console
tox -e release
```

This will check the current version of the `main` branch and trigger
a GitHub continuous integration pipeline to create a new release. The procedure
performs a couple of checks, if theses checks fail please make sure to address
the issues.

## Docker production container
It's best to use the system in production within a dedicated docker container.
You can pull the container from the GitHub container registry:

```console
docker pull ghcr.io/freva-clint/databrowserapi:latest
```

There are two fundamental different options to configure the service.

1. via the `config` ``.toml`` file.
2. via environment variables.

Note, that the order here is important. First, any configuration from the
config file is loaded, only if the configuration wasn't found in the config
file environment variables are evaluated. The following environment
variables can be set:

- ``DEBUG``: Start server in debug mode (1), (default: 0 -> no debug).
- ``API_PORT``: the port the rest service should be running on (default 8080).
- ``API_WORKER``: the number of multi-process work serving the API (default: 8).
- ``SOLR_HOST``: host name of the solr server, host name and port should be
                 separated by a ``:``, for example ``localhost:8983``
- ``SOLR_CORE`` : name of the solr core that contains datasets with multiple
                  versions
- ``MONGO_HOST``: host name of the mongodb server, where query statistics are
                 stored. Host name and port should separated by a ``:``, for
                 example ``localhost:27017``
- ``MONGO_USER``: user name for the mongodb.
- ``MONGO_PASSWORD``: password to log on to the mongodb.
- ``MONGO_DB``: database name of the mongodb instance.

> ``📝`` You can override the path to the default config file using the ``API_CONFIG``
         environment variable. The default location of this config file is
         ``/opt/databrowser/api_config.toml``.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
