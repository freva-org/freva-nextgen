FROM quay.io/condaforge/mambaforge
ARG VERSION
LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"
ENV API_CONFIG=/opt/data-loader/config/data-portal-config.json \
    PYTHONUNBUFFERED=1 \
    API_PORT=40000 \
    API_REDIS_SSL_CERTFILE=/certs/client-cert.pem \
    API_REDIS_SSL_KEYFILE=/certs/client-key.pem \
    API_LOGDIR=/opt/data-loader/log


RUN mkdir -p /docker-entrypoint-initdb.d
WORKDIR /tmp/app
COPY . .
RUN  mkdir -p /opt/data-loader/config /opt/data-loader/log && \
     chmod -R 2777 /opt/data-loader &&\
     mamba install -y -q -c conda-forge --override-channels zarr cfgrib && \
     python3 -m pip install -q --no-color --root-user-action ignore --no-cache-dir . &&\
     python3 -m pip cache purge -q --no-input && \
     mamba clean -y -i -t -l -f

WORKDIR /opt/data-loader
RUN rm -fr /tmp/app

COPY --chmod=0755 docker-scripts/entrypoint.sh /docker-entrypoint-initdb.d/entrypoint.sh
COPY docker-scripts/logging.sh /usr/local/lib/logging.sh

ENTRYPOINT ["/docker-entrypoint-initdb.d/entrypoint.sh"]
CMD ["data-loader-worker"]
