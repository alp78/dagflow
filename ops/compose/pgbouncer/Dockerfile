FROM alpine:3.20

RUN apk add --no-cache bash pgbouncer

COPY ops/compose/pgbouncer/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh \
    && adduser -D -H pgbouncer \
    && mkdir -p /etc/pgbouncer /var/run/pgbouncer /var/log/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/run/pgbouncer /var/log/pgbouncer

USER pgbouncer

EXPOSE 6432

ENTRYPOINT ["/entrypoint.sh"]
