#!/usr/bin/env bash

source /usr/local/lib/logging.sh

create_opensearch_user() {
    log_info "Checking for opensearch user"
    if ! id -u opensearch &>/dev/null; then
        log_info "Creating opensearch user and group"
        groupadd -g 1000 opensearch
        useradd -u 1000 -g opensearch -s /bin/bash -m opensearch
        log_info "Successfully created opensearch user"
    else
        log_info "Opensearch user already exists"
    fi
}

add_opensearch_ism_remove_policy()
{
    log_info "Creating ISM policy to remove items older than 5 days"
    curl -X PUT "localhost:9202/_plugins/_ism/policies/flush_nach_5_tagen" -H 'Content-Type: application/json' -d'{
    "policy": {
        "policy_id": "flush_nach_5_tagen",
        "description": "it will delete items older than 5 days",
        "schema_version": 18,
        "error_notification": null,
        "default_state": "init_state",
        "states": [
        {
            "name": "init_state",
            "actions": [],
            "transitions": [
            {
                "state_name": "delete_state",
                "conditions": {
                "min_index_age": "5d"
                }
            }
            ]
        },
        {
            "name": "delete_state",
            "actions": [
            {
                "retry": {
                "count": 3,
                "backoff": "exponential",
                "delay": "1m"
                },
                "delete": {}
            }
            ],
            "transitions": []
        }
        ],
        "ism_template": [
        {
            "index_patterns": [
            "items_*"
            ],
            "priority": 1
        }
        ]
    }
    }
    '
    log_info "Adding ISM policy to items_* index"
    curl -X POST "localhost:9202/_plugins/_ism/add/items_*" -H 'Content-Type: application/json' -d'
    {
    "policy_id": "flush_nach_5_tagen"
    }
    '
}