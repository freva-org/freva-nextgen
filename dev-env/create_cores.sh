#!/bin/bash

for this_core in $CORE latest; do
    if [ ! -d "/var/solr/data/$this_core" ]; then
        precreate-core $this_core
    fi
    cp /opt/solr/managed-schema.xml /var/solr/data/$this_core/conf/managed-schema.xml
    rm -rf /var/solr/data/$this_core/conf/synonyms.txt
    ln -s /opt/solr/synonyms.txt /var/solr/data/$this_core/conf/synonyms.txt
done

# Create the static core
if [ ! -d "/var/solr/data/static" ]; then
    precreate-core static
fi
cp /opt/solr/static-managed-schema.xml /var/solr/data/static/conf/managed-schema.xml