FROM python:latest as base
ARG VERSION
LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/databrowserAPI"
LABEL org.opencontainers.image.version="$VERSION"
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
    python3 -m pip install flit && flit build
FROM base as final
COPY --from=builder /opt/app/dist /opt/app/dist
RUN python3 -m pip install /opt/app/dist/databrowser*.whl
WORKDIR /opt/databrowser
EXPOSE $API_PORT
USER freva
CMD ["python3", "-m", "databrowser.cli"]
