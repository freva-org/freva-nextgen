#!/usr/bin/env bash
set -o nounset -Eeuo errexit -Eeuo pipefail
source /usr/local/lib/logging.sh

check_env(){

    local var_name="$1"
    shift
    local service_name=$@
    if [[ -z ${!var_name:-} ]];then
        log_error "In order to set up $service_name you must set the '\$${var_name:-}' environment variable."
        exit 1
    fi
}


init() {
    if echo $@|grep -qE 'freva_rest|freva-rest';then
        log_service "Starting freva-rest API"
        check_env API_OIDC_DISCOVERY_URL freva-rest API
    fi
    local command=${1:-}
    shift || true

    case "${command:-}" in
        "")
            check_env API_OIDC_DISCOVERY_URL freva-rest API
            log_service "Starting freva-rest API"
            exec python3 -m freva_rest.cli
            ;;
        "sh"|"bash"|"zsh")
            log_info "Starting container..."
            exec "${command}" "$@"
            ;;
        -*)
            check_env API_OIDC_DISCOVERY_URL freva-rest API
            log_service "Starting freva-rest API"
            exec python3 -m freva_rest.cli "${command}" "$@"
            ;;
        "exec")
            if [ $# -eq 0 ]; then
                log_error "Error: 'exec' provided without a command to execute."
                return 1
            fi
            log_info "Starting container..."
            exec "$@"
            ;;
        *)
            log_info "Starting container..."
            exec "${command}" "$@"
            ;;
    esac
}

main() {
    display_logo
    mkdir -p ${API_LOGDIR:-/var/log/freva-rest-server}
    init "$@"
}

main "$@"
