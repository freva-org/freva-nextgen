# The freva databrowser API

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11-purple.svg)](https://www.python.org/downloads/release/python-311/)
[![Poetry](https://img.shields.io/badge/poetry-1.5.1-purple)](https://python-poetry.org/)
[![Docs](https://img.shields.io/badge/API-Doc-green.svg)](https://freva-clint.github.io/databrowserAPI)

The Freva Databrowser REST API is a powerful tool that enables you to search
for climate and environmental datasets seamlessly in various programming
languages. By generating RESTful requests, you can effortlessly access
collections of various datasets, making it an ideal resource for climate
scientists, researchers, and data enthusiasts.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Docker Development Environment](#docker-development-environment)
- [License](#license)

## Installation

1. Make sure you have Python 3.11 installed.
2. Install Poetry on your system by following the instructions at [Python Poetry](https://python-poetry.org/).
3. Clone this repository:

```console
git clone git@github.com:FREVA-CLINT/databrowserAPI.git
cd databrowserAPI
```

4. Set up the project environment using Poetry:

```console
poetry install --no-root --all-extras
```

Make sure poetry is available in your python environment

## Development Environment
Apache solr is needed run the system in a development environment, here we
set up solr in a docker container using the `docker-compose` command, ensure
you have Docker Compose installed on your system.
Then, run the following command:

```console
docker-compose -f dev-env/docker-compose.yaml up -d --remove-orphans
```

After solr is up and running you can start the REST server the following:

```console
poetry run python run_server.py --config-file api_config.toml --debug --dev --port 7777
```

The ``--debug`` and ``--dev`` flag will make sure that any changes are loaded.
You can choose any port you like.

### Testing

This project uses `pytest` for testing. To run the tests, use the following command:

```console
make test
```

This will start the required services and containers to create the development environment. You can now develop and test the project within this environment.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
