#!/usr/bin/env bash

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
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] [WARNNING]${NC} $* >&2"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] [ERROR]${NC} $* >&2"
}

log_debug() {
    echo -e "${CYAN}[$(date +'%Y-%m-%d %H:%M:%S')] [DEBUG]${NC} $*"
}

log_service() {
    echo -e "${MAGENTA}[$(date +'%Y-%m-%d %H:%M:%S')] [SERVICE]${NC} === $* ==="
}
