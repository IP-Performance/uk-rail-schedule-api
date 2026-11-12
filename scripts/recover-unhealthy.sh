#!/bin/sh
# Recover only containers whose database-backed Docker healthcheck has failed.
# Feed outages (including planned Darwin maintenance) do not affect this check.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(dirname -- "$script_dir")
state_file=/run/uk-rail-schedule-api-recovery.last
cooldown_seconds=${RECOVERY_COOLDOWN_SECONDS:-600}
mysql_socket=${MYSQL_SOCKET_PATH:-/var/run/mysqld/mysqld.sock}

cd "$project_dir"
compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose --env-file ../.env -f docker-compose.yml "$@"
    else
        docker-compose --env-file ../.env -f docker-compose.yml "$@"
    fi
}

unhealthy=""
for service in web syncd; do
    container_id=$(compose ps -q "$service")
    [ -n "$container_id" ] || continue
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id")
    [ "$status" = unhealthy ] && unhealthy="$unhealthy $service"
done

[ -n "$unhealthy" ] || exit 0

now=$(date +%s)
if [ -r "$state_file" ]; then
    last=$(cat "$state_file")
    case "$last" in *[!0-9]*|'') last=0 ;; esac
    [ $((now - last)) -ge "$cooldown_seconds" ] || exit 0
fi

# A missing host socket is an upstream dependency failure; wait for MySQL and
# avoid repeatedly restarting containers. A stale container bind mount is
# repaired by recreating both service containers once the host socket exists.
[ -S "$mysql_socket" ] || exit 0
stale_mount=0
for service in web syncd; do
    container_id=$(compose ps -q "$service")
    [ -n "$container_id" ] || continue
    if ! docker exec "$container_id" test -S "$mysql_socket" 2>/dev/null; then
        stale_mount=1
    fi
done

printf '%s\n' "$now" > "$state_file"
if [ "$stale_mount" -eq 1 ]; then
    logger -t uk-rail-schedule-api-recovery "Recreating web and syncd after unhealthy status and stale MySQL socket mount"
    compose up -d --force-recreate web syncd
else
    # Restart only services which have crossed the configured health threshold.
    # shellcheck disable=SC2086
    logger -t uk-rail-schedule-api-recovery "Restarting unhealthy services:$unhealthy"
    # shellcheck disable=SC2086
    compose restart $unhealthy
fi
