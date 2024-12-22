#!/bin/bash
set -o nounset -Eeuo errexit -Eeuo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

display_logo() {
    echo -e "${BLUE}"
    echo -e "${BLUE} ████████▓▒░ ████████▓▒░  ████████▓▒░ ██▓▒░  ██▓▒░  ███████▓▒░ ${NC}"
    echo -e "${BLUE} ██▓▒░       ██▓▒░   █▓▒░ ██▓▒░       ██▓▒░  ██▓▒░ ██▓▒░  ██▓▒░${NC}"
    echo -e "${BLUE} ██▓▒░       ██▓▒░   █▓▒░ ██▓▒░        ██▓▒▒▓█▓▒░  ██▓▒░  ██▓▒░${NC}"
    echo -e "${BLUE} ███████▓▒░  ███████▓▒░   ███████▓▒░   ██▓▒▒▓█▓▒░  ██████████▓▒░${NC}"
    echo -e "${BLUE} ██▓▒░       ██▓▒░   █▓▒░ ██▓▒░         ██▓▓█▓▒░   ██▓▒░  ██▓▒░${NC}"
    echo -e "${BLUE} ██▓▒░       ██▓▒░   █▓▒░ ██▓▒░         ██▓▓█▓▒░   ██▓▒░  ██▓▒░${NC}"
    echo -e "${BLUE} ██▓▒░       ██▓▒░   █▓▒░ █████████▓▒░   ███▓▒░    ██▓▒░  ██▓▒░${NC}"
    echo -e "${NC}"
    echo -e "${GREEN}================================================================${NC}"
    echo -e "${YELLOW}                    Starting FREVA Services                      ${NC}"
    echo -e "${GREEN}================================================================${NC}"
    echo ""
}

log_info() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] [INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] [WARNNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] [ERROR]${NC} $*"
}

log_debug() {
    echo -e "${CYAN}[$(date +'%Y-%m-%d %H:%M:%S')] [DEBUG]${NC} $*"
}

log_service() {
    echo -e "${MAGENTA}[$(date +'%Y-%m-%d %H:%M:%S')] [SERVICE]${NC} $*"
}

init_mongodb() {
    log_service "=== Initialising MongoDB ==="
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-mongo
    log_info "Starting MongoDB with authentication..."
    /opt/conda/bin/mongod -f /opt/conda/share/freva-rest-server/mongodb/mongod.conf --auth &
}

init_solr() {
    log_service "=== Initialising Solr ==="
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-solr
    log_service "Starting solr service"
    /opt/conda/bin/solr start -force
    SOLR_PORT=${API_SOLR_PORT:-8983}
    until curl -s "http://localhost:${SOLR_PORT}/solr/admin/ping" >/dev/null 2>&1; do
        log_debug "Waiting for Solr to start..."
        sleep 1
    done
    log_info "Solr started successfully"
}

init_redis(){
    log_service "=== Initialising and starting Redis ==="
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-redis
    /opt/conda/bin/redis-server /tmp/redis.conf &

}

init_mysql(){
    log_service "=== Initialising MySQL server  ==="
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-mysql
    log_service "Starting mysql service"
    /opt/conda/bin/mysqld --user=$(whoami) &
}

start_freva_service() {
    local command=$1
    shift || true

    log_service "Starting container..."

    case "${command:-}" in
        "")
            exec python3 -m freva_rest.cli
            ;;
        "sh"|"bash"|"zsh")
            exec "${command}" "$@"
            ;;
        -*)
            exec python3 -m freva_rest.cli "${command}" "$@"
            ;;
        "exec")
            if [ $# -eq 0 ]; then
                log_error "Error: 'exec' provided without a command to execute."
                return 1
            fi
            exec "$@"
            ;;
        *)
            exec "${command}" "$@"
            ;;
    esac
}

main() {
    display_logo
    if [[ "${USE_MONGODB}" == "1" ]]; then
        init_mongodb
        log_info "MongoDB startup completed"
    fi
    if [[ "${USE_SOLR}" == "1" ]]; then
        init_solr
        log_info "Solr startup completed"
    fi
    if [[ "${USE_REDIS}" == "1" ]]; then
        init_redis
        log_info "Redis startup completed."
    fi
    if [[ "${USE_MYSQL}" == "1" ]];then
        init_mysql
        log_info "MySQL startup completed."
    fi
    start_freva_service "$@"
}

main "$@"
