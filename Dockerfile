FROM docker.io/mambaorg/micromamba
USER root
ARG VERSION
ARG CMD=freva-rest-server
LABEL org.freva.service="$CMD"
LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/FREVA-CLINT/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"
ENV    PYTHONUNBUFFERED=1 \
       API_LOGDIR=/opt/${CMD}/logs


RUN mkdir -p /docker-entrypoint-initdb.d
WORKDIR /tmp/app
COPY freva-rest /tmp/app/freva-rest
COPY freva-data-portal-worker /tmp/app/freva-data-portal-worker
COPY docker-scripts  /tmp/app/docker-scripts
RUN  set -xe  && \
    mkdir -p /opt/${CMD}/config $API_LOGDIR /certs /etc/profile.d /usr/local/{lib,bin} && \
    cp docker-scripts/${CMD}-env.sh /etc/profile.d/env-vars.sh && \
    cp docker-scripts/logging.sh /usr/local/lib/logging.sh && \
    cp docker-scripts/entrypoint.sh /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    micromamba install -y -q -c conda-forge --override-channels python && \
    if [ "${CMD}" = "data-loader-worker" ];then\
        PKGNAME=freva-data-portal-worker && \
        micromamba install -y -q -c conda-forge --override-channels \
        zarr \
        cfgrib \
        jq \
        numpy \
        distributed \
        dask \
        netcdf4 \
        xarray \
        rioxarray \
        jupyter-server-proxy \
        bokeh \
        asyncssh \
        requests \
        appdirs \
        rasterio \
        cloudpickle\
        redis-py\
        cryptography; \
    elif [ "${CMD}" = "freva-rest-server" ];then\
        PKGNAME=freva-rest && \
        cp /tmp/app/${PKGNAME}/src/freva_rest/api_config.toml /opt/${CMD}/config && \
        micromamba install -y -q -c conda-forge --override-channels jq cryptography zarr; \
    else \
        echo "Invalid CMD argument: $CMD" && exit 1; \
    fi &&\
    $MAMBA_ROOT_PREFIX/bin/python -m pip install -q --no-color --root-user-action ignore --no-cache-dir ./$PKGNAME &&\
    $MAMBA_ROOT_PREFIX/bin/python -m pip cache purge -q --no-input && \
    chmod -R 2777 $API_LOGDIR /opt/${CMD}/config /certs && \
    micromamba clean -y -i -t -l -f

WORKDIR /opt/${CMD}
RUN rm -fr /tmp/app


ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD []
