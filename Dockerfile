FROM debian:buster

WORKDIR /app

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_SRC=/usr/local/src \
    PIPENV_HIDE_EMOJIS=true \
    PIPENV_NOSPIN=true \
    OPENSSL_CONF=/etc/ssl

RUN apt-get update \
    && apt-get install -y python3-pip

RUN pip3 install https://github.com/harvard-lil/s3mothball/archive/master.zip#egg=s3mothball
