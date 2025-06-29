# Data Loader

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-red.svg)](https://www.python.org/downloads/release/python-312/)
[![Docs](https://img.shields.io/badge/API-Doc-green.svg)](https://freva-org.github.io/freva-nextgen)
[![Tests](https://github.com/freva-org/freva-nextgen/actions/workflows/ci_job.yml/badge.svg)](https://github.com/freva-org/freva-nextgen/actions)
[![Test-Coverage](https://codecov.io/github/freva-org/freva-nextgen/branch/init/graph/badge.svg?token=dGhXxh7uP3)](https://codecov.io/github/freva-org/freva-nextgen)


The Data Loader is a Python package designed to open, chunk, and cache
climate datasets from various storage locations.
By using the power of dask-distributed, this library ensures efficient data
processing and accessibility. Cached data is stored in Redis and can be
accessed via a REST API for Zarr endpoint streaming.

## Features

- Open and process climate datasets from multiple storage locations
- Chunk data for optimized performance
- Cache processed data in Redis
- Access cached data through a REST API for Zarr endpoint streaming

## Installation

> [!CAUTION]
> A manual setup of the service will most likely fail. You should set up this
> service via the [freva-deployment](https://freva-deployment.readthedocs.io/en/latest/)
> routine.
