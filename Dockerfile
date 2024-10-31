FROM ubuntu:22.04 as base
ARG VERSION
ARG DEBIAN_FRONTEND=noninteractive

LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev python3-venv \
        gcc g++ make curl openssl pkg-config python3-setuptools python3-wheel build-essential && \
    rm -rf /var/lib/apt/lists/*

ENV API_CONFIG=/opt/freva-rest/api_config.toml \
    API_PORT=7777 \
    API_WORKER=8 \
    DEBUG=0 \
    # TODO: organize home directories to be able to manage postgress as well
    MONGO_HOME=/opt/mongodb \
    SOLR_HOME=/opt/solr_data \
    JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/mongodb/bin:/opt/java/openjdk/bin:/opt/solr/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    SOLR_LOGS_DIR=/opt/solr_data/logs/solr \
    LOG4J_PROPS=/opt/solr_data/log4j2.xml \
    SOLR_PID_DIR=/opt/solr_data \
    SOLR_JETTY_HOST=0.0.0.0 \
    SOLR_PORT=8983 \
    MONGO_PORT=27017 \
    # TODO: this stuff needs to be gotten from env file
    MONGO_PASSWORD=secret \
    MONGO_DB=search_stats \
    MONGO_USER=mongo \
    # TODO: prevent getting GLIB mongo warning, need to find a better way
    GLIBC_TUNABLES=glibc.pthread.rseq=0

COPY --from=mongo:latest /usr/bin/mongod /usr/bin/mongod
COPY --from=mongo:latest /usr/bin/mongos /usr/bin/mongos
COPY --from=mongo:latest /usr/bin/mongosh /usr/bin/mongosh
COPY --from=mongo:latest /usr/bin/mongoexport /usr/bin/mongoexport
COPY --from=mongo:latest /usr/bin/mongoimport /usr/bin/mongoimport
COPY --from=mongo:latest /lib/ /lib/
COPY --from=mongo:latest /usr/lib/ /usr/lib/

COPY --from=solr:latest /opt/solr/ /opt/solr/
COPY --from=solr:latest /opt/java/ /opt/java/
COPY --from=solr:latest /var/solr/ /var/solr/

#TODO: copy postgtres lib and binaries here
RUN groupadd -r --gid 1000 freva && \
    useradd -r -g freva -u 1000 -s /bin/bash -d /opt/freva-rest freva && \
    mkdir -p /etc/mongodb /opt/app /opt/freva-rest ${SOLR_HOME} ${MONGO_HOME}/data && \
    echo "security:\n  authorization: enabled\n\
storage:\n  dbPath: /opt/mongodb/data\n\
net:\n  port: 27017\n  bindIp: 0.0.0.0\n" > /etc/mongodb/mongod.conf && \
    chown -R freva:freva /opt/freva-rest ${MONGO_HOME}

RUN /opt/solr/docker/scripts/init-var-solr && \
    /opt/solr/docker/scripts/precreate-core latest && \
    /opt/solr/docker/scripts/precreate-core files && \
    find /var/solr -type d -print0 | xargs -0 chmod 0771 && \
    find /var/solr -type f -print0 | xargs -0 chmod 0661 && \
    mv /var/solr ${SOLR_HOME} && \
    ln -s ${SOLR_HOME} /var/solr && \
    chown -R freva:freva ${SOLR_HOME}

FROM base as builder
WORKDIR /opt/app
COPY freva-rest .
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir build && \
    python3 -m pip install . && \
    python3 -m build --sdist --wheel

FROM base as final
WORKDIR /opt/freva-rest
COPY --from=builder /opt/app/dist /opt/app/dist
COPY --chown=freva:freva freva-rest/src/freva_rest/api_config.toml $API_CONFIG
# we need to figure out a new dir for the entrypoint and other config files
COPY --chown=freva:freva dev-env/entrypoint.sh ./
RUN chmod +x ./entrypoint.sh && \
    python3 -m pip install --no-cache-dir /opt/app/dist/freva_rest*.whl

EXPOSE $API_PORT $MONGO_PORT $SOLR_PORT

USER freva
ENTRYPOINT ["./entrypoint.sh"]