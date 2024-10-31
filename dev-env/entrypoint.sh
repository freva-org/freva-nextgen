#!/bin/bash
set -e
#TODO: flush mongo password in the volume and then initialize the new password
if [ ! -d "${MONGO_HOME}/data" ]; then
    mkdir -p ${MONGO_HOME}/data
    chown -R freva:freva ${MONGO_HOME}/data
fi

mongod --dbpath ${MONGO_HOME}/data \
    --port ${MONGO_PORT} \
    --bind_ip 0.0.0.0 &

until mongosh --quiet --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
    echo "Waiting for MongoDB to start..."
    sleep 1
done

mongosh --eval "
    if (db.getSiblingDB('admin').getUser('${MONGO_USER}') == null) {
        db.getSiblingDB('admin').createUser({
            user: '${MONGO_USER}',
            pwd: '${MONGO_PASSWORD}',
            roles: ['root']
        });
        db.getSiblingDB('${MONGO_DB}').createCollection('searches');
    }
"

solr start -force
until curl -s "http://localhost:${SOLR_PORT}/solr/admin/ping" >/dev/null 2>&1; do
    echo "Waiting for Solr to start..."
    sleep 1
done

#TODO: add postgres initialization and wait ping
exec python3 -m freva_rest.cli