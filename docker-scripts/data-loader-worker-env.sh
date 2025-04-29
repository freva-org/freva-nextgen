export API_PORT=${API_PORT:-40000}
export API_REDIS_SSL_CERTFILE=${API_REDIS_SSL_CERTFILE:-/certs/client-cert.pem}
export API_REDIS_SSL_KEYFILE=${API_REDIS_SSL_KEYFILE:-/certs/client-key.pem}
export API_CONFIG=${API_CONFIG:-/opt/data-loader-worker/config/data-portal-cluster-config.json}
export CONTAINER=data-loader-worker
