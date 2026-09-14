ARG PARADEDB_IMAGE=paradedb/paradedb:v0.25.2-pg18
FROM ${PARADEDB_IMAGE}

ARG POSTGRES_VERSION=18.6-1.pgdg13+2

# ParadeDB 0.25.2 was published on PostgreSQL 18.4. Keep its extension and
# bootstrap environment intact while updating the ABI-compatible PostgreSQL
# minor release used by every benchmark backend.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        "libpq5=${POSTGRES_VERSION}" \
        "postgresql-18=${POSTGRES_VERSION}" \
        "postgresql-18-jit=${POSTGRES_VERSION}" \
        "postgresql-client-18=${POSTGRES_VERSION}" \
    && rm -rf /var/lib/apt/lists/* \
    && postgres --version | grep -F 'PostgreSQL) 18.6'
