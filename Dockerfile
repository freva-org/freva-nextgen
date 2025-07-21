FROM docker.io/mambaorg/micromamba
USER root
ARG VERSION
ARG CMD=freva-rest-server
LABEL org.freva.service="$CMD"
LABEL org.opencontainers.image.authors="DRKZ-CLINT"
LABEL org.opencontainers.image.source="https://github.com/freva-org/freva-nextgen/freva-rest"
LABEL org.opencontainers.image.version="$VERSION"
ENV    PYTHONUNBUFFERED=1 \
       API_LOGDIR=/opt/${CMD}/logs


RUN mkdir -p /docker-entrypoint-initdb.d
WORKDIR /tmp/app
COPY freva-rest /tmp/app/freva-rest
COPY freva-data-portal-worker /tmp/app/freva-data-portal-worker
COPY docker-scripts  /tmp/app/docker-scripts
RUN  set -xe  && \
    apt -y update &&\
    DEBIAN_FRONTEND=noninteractive apt -y install sssd libnss-sss libpam-sss \
    sssd-common sssd-tools &&\
    rm -rf /var/lib/apt/lists/* && \
     printf "\n\
auth required pam_sss.so\n\
account required pam_sss.so\n\
password required pam_sss.so\n\
session required pam_sss.so\n\
" > /etc/pam.d/login &&\
    mkdir -p /opt/${CMD}/config $API_LOGDIR /certs /etc/profile.d /usr/local/{lib,bin} && \
    cp /tmp/app/docker-scripts/${CMD}-env.sh /etc/profile.d/env-vars.sh && \
    cp /tmp/app/docker-scripts/logging.sh /usr/local/lib/logging.sh && \
    cp /tmp/app/docker-scripts/entrypoint.sh /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    micromamba install -y -q -c conda-forge --override-channels python && \
    if [ "${CMD}" = "data-loader-worker" ];then\
        PKGNAME=freva-data-portal-worker && \
        micromamba install -y -q -c conda-forge --override-channels \
        appdirs \
        asyncssh \
        cfgrib \
        cloudpickle \
        cryptography \
        bokeh \
        dask \
        distributed \
        h5netcdf \
        jq \
        jupyter-server-proxy \
        netcdf4 \
        numpy \
        rasterio \
        redis-py \
        requests \
        rioxarray \
        watchfiles \
        xarray \
        zarr; \
    elif [ "${CMD}" = "freva-rest-server" ];then\
        PKGNAME=freva-rest && \
        cp /tmp/app/${PKGNAME}/src/freva_rest/api_config.toml /opt/${CMD}/config && \
        micromamba install -y -q -c conda-forge --override-channels \
        aiohttp \
        cloudpickle \
        cryptography \
        fastapi \
        fastapi-third-party-auth \
        email-validator \
        httpx \
        jq \
        motor>=3.6 \
        pyjwt \
        pymongo>=4.9 \
        python-dotenv \
        python-dateutil \
        python-multipart \
        redis-py \
        requests \
        rich \
        rich-argparse \
        setuptools \
        tomli \
        typing_extensions \
        uvicorn \
        tomli \
        zarr; \
    else \
        echo "Invalid CMD argument: $CMD" && exit 1; \
    fi &&\
    chmod -R 2777 $API_LOGDIR /opt/${CMD}/config /certs && \
    micromamba clean -y -i -t -l -f
RUN set -ex && \
    if [ "${CMD}" = "data-loader-worker" ];then\
        PKGNAME=freva-data-portal-worker; \
    else \
        PKGNAME=freva-rest; \
    fi &&\
    $MAMBA_ROOT_PREFIX/bin/python -m pip install --no-deps --no-color --root-user-action ignore --no-cache-dir ./$PKGNAME &&\
    $MAMBA_ROOT_PREFIX/bin/python -m pip cache purge -q --no-input

WORKDIR /opt/${CMD}
RUN rm -fr /tmp/app


ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD []
