FROM python:latest as base
LABEL maintainer="DRKZ-CLINT"
LABEL repository="https://github.com/FREVA-CLINT/databrowserAPI"
ENV API_CONFIG=/opt/databrowser/api_config.toml \
    API_PORT=8080\
    API_WORKER=8\
    DEBUG=0
USER root
RUN set -e &&\
    mkdir -p /opt/app &&\
    groupadd -r --gid 1000 freva &&\
    mkdir -p /opt/databrowser &&\
    adduser --uid 1000 --gid 1000 --gecos "Default user" \
    --shell /bin/bash --disabled-password freva --home /opt/databrowser &&\
    chown -R freva:freva /opt/databrowser
COPY --chown=freva:freva src/databrowser/api_config.toml $API_CONFIG
FROM base as builder
COPY . /opt/app
WORKDIR /opt/app
RUN set -e &&\
    python3 -m ensurepip -U &&\
    python3 -m pip install -q poetry &&\
    poetry config virtualenvs.in-project true && \
    poetry install -q --only main --no-root --no-directory --no-interaction &&\
    poetry build
FROM base as final
COPY --from=builder /opt/app/.venv /opt/app/python
COPY --from=builder /opt/app/dist /opt/app/dist
RUN /opt/app/python/bin/python3 -m pip install /opt/app/dist/databrowser*.whl
WORKDIR /opt/databrowser
EXPOSE $API_PORT
USER freva
CMD ["/opt/app/python/bin/python3", "-m", "databrowser.cli"]
