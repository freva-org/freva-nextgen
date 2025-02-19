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
    echo -e "${MAGENTA}[$(date +'%Y-%m-%d %H:%M:%S')] [SERVICE]${NC} === $* ==="
}

check_env(){

    local var_name="$1"
    shift
    local service_name=$@
    if [[ -z ${!var_name:-} ]];then
        log_error "In order to set up $service_name you must set the '\$${var_name:-}' environment variable." >&2
        exit 1
    fi
}


init_mongodb() {
    log_info "Initialising MongoDB"
    for env in API_MONGO_USER API_MONGO_PASSWORD; do
        check_env $env mongoDB
    done
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-mongo
    log_info "Starting MongoDB with authentication..."
    /opt/conda/bin/mongod \
        -f /opt/conda/share/freva-rest-server/mongodb/mongod.yaml \
        --auth \
        --cpu > /logs/mongodb.log 2>&1 &
}

init_solr() {
    log_service "Initialising Solr"
    ulimit -n 65000 || echo "Warning: Unable to set ulimit -n 65000"
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-solr
    log_info "Starting solr service"
    nohup /opt/conda/bin/solr start -force > /logs/solr.log 2>&1 &
    SOLR_PORT=${API_SOLR_PORT:-8983}
    timeout 60 bash -c 'until curl -s http://localhost:'"$SOLR_PORT"'/solr/admin/ping;do sleep 2; done' || {
            echo "Error: Solr did not start within 60 seconds." >&2
            exit 1
    }
}

init_redis(){
    log_service "Initialising and starting Redis"
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-redis
    nohup /opt/conda/bin/redis-server /tmp/redis.conf  > /logs/redis.log 2>&1 &
}

init_mysql(){
    log_info "Initialising MySQL server"
    for env in MYSQL_USER MYSQL_PASSWORD MYSQL_ROOT_PASSWORD MYSQL_DATABASE; do
        check_env $env MySQL
    done
    /bin/bash /opt/conda/libexec/freva-rest-server/scripts/init-mysql
    log_info "Starting mysql service"
    nohup /opt/conda/bin/mysqld --user=$(whoami) > /logs/mysqld.log 2>&1 &
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

    if [[ "${USE_MONGODB:-0}" == "1" ]]; then
        init_mongodb
        log_service "MongoDB startup completed"
    fi
    if [[ "${USE_SOLR:-0}" == "1" ]]; then
        init_solr
        log_service "Solr startup completed"
    fi
    if [[ "${USE_REDIS:-0}" == "1" ]]; then
        init_redis
        log_service "Redis startup completed."
    fi
    if [[ "${USE_MYSQL:-0}" == "1" ]];then
        init_mysql
        log_service "MySQL startup completed."
    fi
    init "$@"
}

main "$@"
