#!/usr/bin/env bash
set -Eeuo pipefail

export PATH=/opt/conda/bin:/opt/conda/condabin:$PATH

source /etc/profile.d/env-vars.sh
source /usr/local/lib/logging.sh

check_env() {
    local var_name="$1"
    local service_name="$2"

    if [[ -z "${!var_name:-}" ]]; then
        log_error "To start $service_name, you must set the '\$${var_name}' environment variable."
        exit 1
    fi
}

json_get() {
    local json="$1"
    local filter="$2"

    printf '%s' "$json" | jq -r "$filter // empty"
}

export_if_not_empty() {
    local name="$1"
    local value="$2"

    if [[ -n "$value" ]]; then
        export "$name=$value"
    else
        unset "$name"
    fi
}

export_json_value_if_not_empty() {
    local json="$1"
    local env_name="$2"
    local filter="$3"
    local value

    value="$(json_get "$json" "$filter")"
    export_if_not_empty "$env_name" "$value"
}

write_json_value_to_file_if_not_empty() {
    local json="$1"
    local env_name="$2"
    local content_filter="$3"
    local path_filter="$4"
    local default_path="$5"
    local content
    local path

    content="$(json_get "$json" "$content_filter")"

    if [[ -z "$content" ]]; then
        unset "$env_name"
        return 0
    fi

    path="$(json_get "$json" "$path_filter")"
    path="${path:-$default_path}"

    mkdir -p "$(dirname "$path")"
    printf '%s\n' "$content" > "$path"
    chmod 0600 "$path"

    export "$env_name=$path"
}

configure_redis_from_cache_config() {
    local decoded_cache_config

    if [[ -z "${CACHE_CONFIG:-}" ]]; then
        return 0
    fi

    decoded_cache_config="$(printf '%s' "$CACHE_CONFIG" | base64 --decode)"

    # Redis connection/config values. Missing, null, or empty values are unset.
    export_json_value_if_not_empty "$decoded_cache_config" \
        API_REDIS_USER '.user'
    export_json_value_if_not_empty "$decoded_cache_config" \
        API_REDIS_PASSWORD '.passwd'
    export_json_value_if_not_empty "$decoded_cache_config" \
        API_REDIS_HOST '.host'
    export_json_value_if_not_empty "$decoded_cache_config" \
        API_REDIS_SCHEDULER_HOST '.scheduler_host'
    export_json_value_if_not_empty "$decoded_cache_config" \
        API_REDIS_CACHE_EXP '.cache_exp'

    # Redis TLS files. The file path env vars are only exported when the
    # corresponding file content exists.
    write_json_value_to_file_if_not_empty "$decoded_cache_config" \
        API_REDIS_SSL_CERTFILE \
        '.ssl_cert' \
        '.ssl_certfile' \
        '/tmp/redis/server.crt'

    write_json_value_to_file_if_not_empty "$decoded_cache_config" \
        API_REDIS_SSL_KEYFILE \
        '.ssl_key' \
        '.ssl_keyfile' \
        '/tmp/redis/server.key'
}

run_freva_rest() {
    log_service "Starting freva-rest API"
    configure_redis_from_cache_config
    python3 -m freva_rest.cli "$@"
}

run_data_loader() {
    log_service "Starting data-loader"
    configure_redis_from_cache_config
    python3 -m data_portal_worker "$@"
}

dispatch_command() {
    local command="${1:-${CONTAINER:-}}"
    shift || true

    case "$command" in
        freva-rest-server|freva_rest|freva-rest)
            run_freva_rest "$@"
            ;;
        data-loader-worker|data_loader|data-loader)
            run_data_loader "$@"
            ;;
        ""|sh|bash|zsh)
            log_info "Starting container shell..."
            "${command:-bash}" "$@"
            ;;
        exec)
            if [[ $# -eq 0 ]]; then
                log_error "'exec' used without a command"
                exit 1
            fi
            log_info "Executing custom command..."
            "$@"
            ;;
        -*)
            # Assume CLI flags for freva_rest by default if CONTAINER says so.
            if [[ "${CONTAINER:-}" == "freva-rest-server" ]]; then
                run_freva_rest "$command" "$@"
            else
                run_data_loader "$command" "$@"
            fi
            ;;
        *)
            log_info "Starting custom command: $command"
            "$command" "$@"
            ;;
    esac
}

main() {
    display_logo

    mkdir -p "${API_LOGDIR:-/opt/${CONTAINER:-app}/logs}"
    dispatch_command "$@"
}

main "$@"
