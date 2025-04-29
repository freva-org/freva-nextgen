# Freva server - client structure

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/pyversions/freva-client.svg)](https://pypi.org/project/freva-client/)
[![Docs](https://img.shields.io/badge/API-Doc-green.svg)](https://freva-clint.github.io/freva-nextgen)
[![Tests](https://github.com/FREVA-CLINT/freva-nextgen/actions/workflows/ci_job.yml/badge.svg)](https://github.com/FREVA-CLINT/freva-nextgen/actions)
[![Test-Coverage](https://codecov.io/github/FREVA-CLINT/freva-nextgen/branch/init/graph/badge.svg?token=dGhXxh7uP3)](https://codecov.io/github/FREVA-CLINT/freva-nextgen)


This is a multi-part repository it contains code for:

- The *freva-rest service* defining rest endpoints
  that make up the freva server services
- The *freva-client* library that provide command line interfaces and python
  libraries for their rest service counterparts.
- The *freva-data-loader-portal* that implements rules of how to open different
  data sources and stream them via zarr.

## Installation for development

1. Make sure you have Python 3.8+ installed.
2. Clone this repository:

    ```console
    git clone --recursive git@github.com:FREVA-CLINT/freva-nextgen.git
    cd freva-nextgen
    ```

3. Install all components:

    ```console
        make install
    ```

    Alternatively you can create an encapsulated conda enviroment:
    ```console
        conda create -n freva-nextgen cfgrib
        conda run -n freva-nextgen make install
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

if you use `podman`:

```console
python -m pip install podman-compose
podman-compose -f dev-env/docker-compose.yaml up -d --remove-orphans
```

This will start the required services and containers to create the development
environment. You can now develop and test the project within this environment.

After the containers are up and running you will need to
[install the REST server](freva-rest/README.md) and start it
doing the following:

```console
python run_server.py --config dev-env/api_config.toml --debug --dev -p 7777 -f
```

The ``--debug`` and ``--dev`` flag will make sure that any changes are loaded.
You can choose any port you like. Furthermore the ``--dev`` flag will pre
load any existing test data. If you don't like that simply do not pass the
``--dev`` flag.


### Test ldap instance
The dev system sets up a small LDAP server for testing. The following users
in this ldap server are available:

- uid: ``johndoe``, password: ``johndoe123``
- uid: ``janedoe``, password: ``janedoe123``
- uid: ``alicebrown``, password: ``alicebrown123``
- uid: ``bobsmith``, password: ``bobsmith123``
- uid: ``lisajones``, password: ``lisajones123``

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

### Creating a test docker container for freva-rest-api and data-loader-worker
This repository defines two docker images that can be used for production

- `freva-rest-api` (restAPI)
- `data-loader-worker` (zarr streaming)


These images are automatically build via the GitHub CI pipeline.
Yet for debugging they can be built using the following commands:

- First setup mulitarch build:
```console
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes && \
 docker buildx create --name multiarch --use && \
 docker buildx inspect --bootstrap
```

- Build for the `data-loader-worker` image for the target platforms
```console
docker buildx build --no-cache --platform linux/amd64,linux/arm64,linux/ppc64le\
    -t ghcr.io/freva-clint/freva-data-loader:latest  \
    -t ghcr.io/freva-clint/freva-data-loader:<VERSION> \
    --build-arg=VERSION=<VERSION> \
    --build-arg=CMD=data-loader-worker .
```

- Build for the `freva-rest-api` image for the target platforms
```console
docker buildx build --no-cache --platform linux/amd64,linux/arm64,linux/ppc64le\
    -t ghcr.io/freva-clint/freva-rest-api:latest  \
    -t ghcr.io/freva-clint/freva-rest-api:<VERSION> \
    --build-arg=VERSION=<VERSION> \
    --build-arg=CMD=freva-rest-server .
```

> If you are using podman make sure
> [buildah and qemu are set up for your OS.](https://danmanners.com/posts/2022-01-buildah-multi-arch/).
> You can then replace the `docker buildx` commands with `buildah`.

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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
