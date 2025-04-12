mkdir -p /data/MACDA/redpanda/data && \
mkdir -p /data/MACDA/timescaledb/data && \
mkdir -p /data/MACDA/pgadmin && \
mkdir -p /data/MACDA/desktop/UserHome && \
mkdir -p /data/MACDA/desktop/UserData && \
mkdir -p /data/MACDA/mock-data/redpanda/data && \
mkdir -p /data/MACDA/mock-data/desktop/UserHome && \
mkdir -p /data/MACDA/mock-data/desktop/UserData && \
chown -R 101:101 /data/MACDA/redpanda/data && \
chown -R 1000:1000 /data/MACDA/timescaledb/data && \
chown -R 5050:5050 /data/MACDA/pgadmin && \
chown -R 1000:1000 /data/MACDA/desktop/UserHome && \
chown -R 1000:1000 /data/MACDA/desktop/UserData && \
chown -R 101:101 /data/MACDA/mock-data/redpanda/data && \
chown -R 1000:1000 /data/MACDA/mock-data/desktop/UserHome && \
chown -R 1000:1000 /data/MACDA/mock-data/desktop/UserData
