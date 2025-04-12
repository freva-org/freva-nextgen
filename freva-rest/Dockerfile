FROM python:3-slim
ARG VERSION
LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"
ENV API_CONFIG=/opt/freva-rest/api_config.toml \
    PYTHONUNBUFFERED=1 \
    API_SOLR_CORE=files \
    API_PORT=7777 \
    API_REDIS_SSL_CERTFILE=/certs/client-cert.pem \
    API_REDIS_SSL_KEYFILE=/certs/client-key.pem \
    API_SOLR_HOST=localhost:8983 \
    API_MONGO_HOST=localhost:27017 \
    API_OIDC_CLIENT_ID=freva \
    API_MONGO_DB=search_stats \
    API_LOGDIR=/var/log/freva-rest-server


RUN mkdir -p /docker-entrypoint-initdb.d
WORKDIR /tmp/app
COPY . .
RUN  python3 -m pip install -q --no-color --root-user-action ignore --no-cache-dir . &&\
     python3 -m pip cache purge -q --no-input

WORKDIR /opt/freva-rest
COPY src/freva_rest/api_config.toml $API_CONFIG

RUN rm -fr /tmp/app

COPY --chmod=0755 docker-scripts/entrypoint.sh /docker-entrypoint-initdb.d/entrypoint.sh
COPY docker-scripts/logging.sh /usr/local/lib/logging.sh
RUN mkdir -p /var/data /var/log/freva-rest-server && \
    chmod -R 2777 /var/log /var/data

ENTRYPOINT ["/docker-entrypoint-initdb.d/entrypoint.sh"]
CMD ["freva-rest-server"]
