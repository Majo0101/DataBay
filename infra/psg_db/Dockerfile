FROM postgres:16-alpine

# Runs once on first container start (new data dir)
COPY init/ /docker-entrypoint-initdb.d/