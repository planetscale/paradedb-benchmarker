FROM postgres:18.6-bookworm AS build

ARG PG_TEXTSEARCH_VERSION=1.4.0
ARG PG_TEXTSEARCH_COMMIT=7a932505b537d50ad8d2d068a053cd1dd7b646ea

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch "v${PG_TEXTSEARCH_VERSION}" \
        https://github.com/timescale/pg_textsearch.git /tmp/pg_textsearch \
    && test "$(git -C /tmp/pg_textsearch rev-parse HEAD)" = "${PG_TEXTSEARCH_COMMIT}" \
    && make -C /tmp/pg_textsearch -j"$(nproc)" \
    && make -C /tmp/pg_textsearch DESTDIR=/opt/pg_textsearch install

FROM postgres:18.6-bookworm

COPY --from=build /opt/pg_textsearch/usr/lib/postgresql/18/lib/pg_textsearch.so /usr/lib/postgresql/18/lib/pg_textsearch.so
COPY --from=build /opt/pg_textsearch/usr/share/postgresql/18/extension/pg_textsearch* /usr/share/postgresql/18/extension/
