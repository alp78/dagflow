#!/usr/bin/env bash
set -euo pipefail

cat >/etc/pgbouncer/pgbouncer.ini <<EOF
[databases]
${POSTGRES_DB}=host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB}

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
unix_socket_dir = /var/run/pgbouncer
auth_type = plain
auth_file = /etc/pgbouncer/userlist.txt
admin_users = ${POSTGRES_USER}
pool_mode = ${PGBOUNCER_POOL_MODE}
max_client_conn = ${PGBOUNCER_MAX_CLIENT_CONN}
default_pool_size = ${PGBOUNCER_DEFAULT_POOL_SIZE}
server_reset_query = DISCARD ALL
ignore_startup_parameters = extra_float_digits
EOF

cat >/etc/pgbouncer/userlist.txt <<EOF
"${POSTGRES_USER}" "${POSTGRES_PASSWORD}"
EOF

exec pgbouncer /etc/pgbouncer/pgbouncer.ini
