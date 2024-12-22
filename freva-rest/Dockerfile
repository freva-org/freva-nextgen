FROM quay.io/condaforge/mambaforge AS base
ARG VERSION

LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"

ENV API_CONFIG=/opt/freva-rest/api_config.toml\
    PYTHONUNBUFFERED=1\
    CONDA_PREFIX=/opt/conda\
    API_SOLR_CORE=files\
    API_PORT=7777\
    API_SOLR_HOST=localhost:8983\
    API_MONGO_HOST=localhost:27017\
    API_OIDC_CLIENT_ID=freva\
    API_MONGO_DB=search_stats\
    # disable as default
    USE_MONGODB=0\
    USE_SOLR=0\
    USE_REDIS=0\
    USE_MYSQL=0

FROM base AS builder
WORKDIR /opt/app
COPY . .
RUN python3 -m pip install --upgrade pip --break-system-packages && \
    python3 -m pip install build --break-system-packages && \
    python3 -m pip install . --break-system-packages && \
    python3 -m build --sdist --wheel

FROM base AS final
WORKDIR /opt/freva-rest
COPY --from=builder /opt/app/dist /opt/app/dist
COPY src/freva_rest/api_config.toml $API_CONFIG
ENV PATH=/opt/conda/bin:$PATH
RUN mkdir /docker-entrypoint-initdb.d && \
    mamba install -y -q -c conda-forge --override-channels freva-rest-server
COPY entrypoint.sh /docker-entrypoint-initdb.d/entrypoint.sh
RUN chmod +x /docker-entrypoint-initdb.d/entrypoint.sh && \
    /opt/conda/bin/python3 -m pip install /opt/app/dist/freva_rest*.whl --break-system-packages

# VOLUME /opt/conda/var/freva-server/data/mongodb
# VOLUME /opt/conda/libexec/apache-solr/server/solr
# VOLUME /opt/conda/data
ENTRYPOINT ["/docker-entrypoint-initdb.d/entrypoint.sh"]
