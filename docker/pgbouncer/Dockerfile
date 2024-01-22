FROM alpine:3.17
LABEL maintainer="imperialgenomicsfacility"
LABEL version="pgbouncer_v1.19.0"
LABEL description="Docker image for Pgbouncer"
ARG VERSION=1.19.0
RUN \
  # security
  apk add -U --no-cache --upgrade busybox && \
  # Download
  apk add -U --no-cache \
    autoconf \
    autoconf-doc \
    automake \
    c-ares \
    c-ares-dev \
    udns \
    udns-dev \
    wget \
    gcc \
    libc-dev \
    libevent \
    libevent-dev \
    libtool \
    make \
    openssl-dev \
    file \
    pkgconfig \
    postgresql-client && \
  rm -rf /var/lib/apt/lists/*
RUN wget -O  /tmp/pgbouncer-$VERSION.tar.gz \
                http://www.pgbouncer.org/downloads/files/$VERSION/pgbouncer-$VERSION.tar.gz && \
        cd /tmp && \
        tar -xzf pgbouncer-$VERSION.tar.gz && \
        cd pgbouncer-$VERSION && \
        ./configure --prefix=/usr/local --with-cares && \
        make && \
        make install && \
        cd /tmp && \
        rm -f pgbouncer-$VERSION.tar.gz
USER nobody
WORKDIR /etc/pgbouncer
WORKDIR /var/log/pgbouncer
ENTRYPOINT ["sh","-c"]
CMD ["/bin/bash"]