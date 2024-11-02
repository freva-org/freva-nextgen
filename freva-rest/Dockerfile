FROM ubuntu:22.04 as base
ARG VERSION
ARG DEBIAN_FRONTEND=noninteractive

LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"

# TODO: specific version of all pre-requisites and services has to be defined
# pre-requisites
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev python3-venv \
    gcc g++ make curl openssl pkg-config \
    python3-setuptools python3-wheel build-essential \
    gnupg2 wget software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# psql
RUN wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y postgresql postgresql-contrib postgresql-client git && \
    apt-get clean

# mongodb
COPY --from=mongo:latest /usr/bin/mongod /usr/bin/mongod
COPY --from=mongo:latest /usr/bin/mongos /usr/bin/mongos
COPY --from=mongo:latest /usr/bin/mongosh /usr/bin/mongosh
COPY --from=mongo:latest /usr/bin/mongoexport /usr/bin/mongoexport
COPY --from=mongo:latest /usr/bin/mongoimport /usr/bin/mongoimport
COPY --from=mongo:latest /lib/ /lib/
COPY --from=mongo:latest /usr/lib/ /usr/lib/

# solr
COPY --from=solr:latest /opt/solr/ /opt/solr/
COPY --from=solr:latest /opt/java/ /opt/java/
COPY --from=solr:latest /var/solr/ /var/solr/
RUN mkdir -p /tmp/freva && \
    git clone https://github.com/FREVA-CLINT/freva-service-config.git /tmp/freva/freva-service-config && \
    mkdir -p /docker-entrypoint-initdb.d /usr/local/bin && \
    cp /tmp/freva/freva-service-config/solr/managed-schema.xml /opt/solr/managed-schema.xml && \
    cp /tmp/freva/freva-service-config/solr/create_cores.sh /docker-entrypoint-initdb.d/create_cores.sh && \
    cp /tmp/freva/freva-service-config/solr/synonyms.txt /opt/solr/synonyms.txt && \
    cp /tmp/freva/freva-service-config/solr/daily_backup.sh /usr/local/bin/daily_backup && \
    cp /tmp/freva/freva-service-config/mongo/mongo-userdata-init.js /docker-entrypoint-initdb.d/mongo-userdata-init.js

# main env
ENV API_CONFIG=/opt/freva-rest/api_config.toml \
    DEBUG=0 \
    API_WORKER=8 \
    MONGO_HOME=/opt/mongodb \
    SOLR_HOME=/opt/solr_data \
    JAVA_HOME=/opt/java/openjdk \
    SOLR_CORE=files \
    COLUMNS=140 \
    SOLR_LOGS_DIR=/opt/solr_data/logs/solr \
    LOG4J_PROPS=/opt/solr_data/log4j2.xml \
    SOLR_PID_DIR=/opt/solr_data \
    SOLR_JETTY_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1 \
    PGDATA=/var/lib/postgresql/data \
    CORE=files \
    NUM_BACKUPS=7 \
    SOLR_HEAP=4g \
    PATH="/opt/mongodb/bin:/opt/java/openjdk/bin:/opt/solr/bin:$PATH" \
    GLIBC_TUNABLES=glibc.pthread.rseq=0 \
    #ports
    API_PORT=7777 \
    SOLR_PORT=8983 \
    MONGO_PORT=27017 \
    POSTGRES_PORT=5432 \
    # usr pass
    MONGO_USER=mongo \
    MONGO_PASSWORD=secret \
    POSTGRES_USER=freva \
    POSTGRES_PASSWORD=secret \
    # db
    MONGO_DB=search_stats \
    MONGO_INITDB_DATABASE=search_stats \
    POSTGRES_DB=freva_db \
    API_URL=http://www.example.de/ \
    # oidc
    OIDC_CLIENT_ID=freva \
    OIDC_URL=http://keycloak:8080/realms/freva/.well-known/openid-configuration \
    # hosts
    REDIS_HOST=redis://localhost:6379 \
    REDIS_SSL_CERTFILE=/certs/client-cert.pem \
    REDIS_SSL_KEYFILE=/certs/client-key.pem \
    SOLR_HOST=localhost:8983 \
    MONGO_HOST=localhost:27017

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
COPY . .
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install build && \
    python3 -m pip install . && \
    python3 -m build --sdist --wheel

FROM base as final
WORKDIR /opt/freva-rest
COPY --from=builder /opt/app/dist /opt/app/dist
COPY --chown=freva:freva src/freva_rest/api_config.toml $API_CONFIG
COPY --chown=freva:freva entrypoint.sh ./
RUN chmod +x ./entrypoint.sh && \
    python3 -m pip install /opt/app/dist/freva_rest*.whl

RUN mkdir -p /var/lib/postgresql/data && chown -R freva:freva /var/lib/postgresql/data
RUN mkdir -p /var/run/postgresql && chown -R freva:freva /var/run/postgresql
RUN mkdir -p /data/db && chown -R freva:freva /data/db
RUN mkdir -p /opt/freva-rest/mongodb/log && chown -R freva:freva /opt/freva-rest/mongodb/log

EXPOSE $API_PORT $MONGO_PORT $SOLR_PORT $POSTGRES_PORT

USER freva
ENTRYPOINT ["./entrypoint.sh"]