# Freva server - client structure

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/pyversions/freva-client.svg)](https://pypi.org/project/freva-client/)
[![Docs](https://img.shields.io/badge/API-Doc-green.svg)](https://freva-clint.github.io/freva-nextgen)
[![Tests](https://github.com/FREVA-CLINT/freva-nextgen/actions/workflows/ci_job.yml/badge.svg)](https://github.com/FREVA-CLINT/freva-nextgen/actions)
[![Test-Coverage](https://codecov.io/github/FREVA-CLINT/freva-nextgen/branch/init/graph/badge.svg?token=dGhXxh7uP3)](https://codecov.io/github/FREVA-CLINT/freva-nextgen)

This repository contains the *freva-rest services* defining rest endpoints
that make up the freva server services as well as the client
services that provide command line interfaces and python libraries for their
rest service counterparts.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Development Environment](#development-environment)
- [Testing](#testing)
- [License](#license)

## Installation for development

1. Make sure you have Python 3.8+ installed.
2. Clone this repository:

```console
git clone git@github.com:FREVA-CLINT/freva-nextgen.git
cd freva-nextgen
```

3. Install all components:

```console
python -m pip install -e cryptography tox freva-rest freva-client freva-data-portal-worker
```

4. Generate a new pair of self signed certificates

```console
python run_server.py --gen-certs
```

### Development Environment
Various services, such as apache solr are needed to run the rest services system
in a development environment. Here we set up these services in a containers
using the `docker-compose` or `podman-compose` command, ensure
you have `docker-compose` or `podman-compose` installed on your system.
Then, run the following command:

```console
docker-compose -f dev-env/docker-compose.yaml up -d --remove-orphans
```

if you use `podman-compose`:

```console
podman-compose -f dev-env/docker-compose.yaml up -d --remove-orphans
```

This will start the required services and containers to create the development
environment. You can now develop and test the project within this environment.

After the containers are up and running you can start the REST server the following:

```console
python run_server.py -c api_config.toml --debug --dev
```

The ``--debug`` and ``--dev`` flag will make sure that any changes are loaded.
You can choose any port you like. Furthermore the ``--dev`` flag will pre
load any existing test data. If you don't like that simply do not pass the
``--dev`` flag.


## Testing

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
python -m pip install -e freva-rest[tests] freva-client freva-data-portal-worker
pytest -vv ./tests
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

## Freva-client production installation
Installing the freva-client library is easy:

```console
python -m pip install freva-client
```

## Freva-rest production docker container
It's best to use the system in production within a dedicated docker container.
You can pull the container from the GitHub container registry:

```console
docker pull ghcr.io/freva-clint/freva-rest:latest
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
- ``API_URL``: url of the machine that runs of the rest api
- ``API_CACHE_EXP``: expiry time in seconds of the cached data
- ``REDIS_HOST``: Host and port of the redis cache
                  Host name and port should separated by a ``:``, for
                  example ``localhost:5672``
- ``REDIS_PASS``: Password for the redis connection.
- ``REDIS_USER``: Username for the redis connection.
- ``REDIS_SSL_CERTFILE``: Path to the TSL certificate file used to encrypt
                          the redis connection.
- ``REDIS_SSL_KEYFILE``: Path to the TSL key file used to encrypt the redis
                         connection.
> ``📝`` You can override the path to the default config file using the ``API_CONFIG``
         environment variable. The default location of this config file is
         ``/opt/databrowser/api_config.toml``.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
