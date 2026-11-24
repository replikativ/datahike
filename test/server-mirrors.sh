#!/usr/bin/env bash
# Exercise named mirror workers from the same image as the writable server.
set -euo pipefail

image=${1:?usage: server-mirrors.sh DATAHIKE_IMAGE}
publisher_image=${PG_MIRROR_TEST_IMAGE:-postgres:18.6}
scratch=$(mktemp -d "${PG_MIRROR_TEST_TMPDIR:-$PWD}/.mirror-smoke.XXXXXXXX")
chmod 755 "$scratch"
prefix="dh-mirrors-${scratch##*.}"
network="$prefix"
publisher="$prefix-publisher"
native="$prefix-native"
analytics="$prefix-analytics"
metrics="$prefix-metrics"
token=mirror-smoke-http
reader_password=mirror-smoke-reader
publisher_password=mirror-smoke-publisher

cleanup() {
  if [[ $1 != 0 ]]; then
    for name in "$publisher" "$native" "$analytics" "$metrics"; do
      docker logs --tail 24 "$name" 2>&1 || true
    done
  fi
  docker unpause "$publisher" >/dev/null 2>&1 || true
  docker rm -fv "$analytics" "$metrics" "$native" "$publisher" >/dev/null 2>&1 || true
  docker volume rm "$analytics" "$metrics" "$native" "$publisher" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap 'cleanup $?' EXIT

await() {
  for ((attempt=0; attempt<200; attempt++)); do
    if "$@"; then return; fi
    sleep 0.5
  done
  printf 'Timed out waiting for %s\n' "$1" >&2
  return 1
}

published_port() { docker port "$1" "$2/tcp" | sed -n '1s/.*://p'; }
source_sql() { docker exec -i "$publisher" psql -XAtqv ON_ERROR_STOP=1 -U postgres -d postgres "$@"; }
source_ready() { docker exec "$publisher" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; }

native_sql() {
  docker run --rm --network "container:$native" "$publisher_image" \
    psql -h 127.0.0.1 -U datahike -d local -XAtqv ON_ERROR_STOP=1 "$@"
}

mirror_sql() {
  local db=$1 port=$2
  shift 2
  PGPASSWORD="$reader_password" PGCONNECT_TIMEOUT=2 PGSSLMODE=verify-full PGSSLROOTCERT="$scratch/ca.crt" \
    psql -XAtqv ON_ERROR_STOP=1 -h localhost -p "$port" -U reader -d "$db" "$@"
}

mirror_result() {
  local db=$1 port=$2 query=$3 expected=$4
  [[ $(mirror_sql "$db" "$port" -c "$query" 2>/dev/null || true) == "$expected" ]]
}

start_mirror() {
  local name=$1 container=$2
  docker run --detach --name "$container" --network "$network" --no-healthcheck \
    --publish 127.0.0.1::15432 --env 'JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2 -Xmx1g' \
    --volume "$container:/var/lib/datahike" \
    --volume "$scratch/server.edn:/etc/datahike/server.edn:ro" \
    --volume "$scratch/$name.url:/run/secrets/$name.url:ro" \
    --volume "$scratch/listener.edn:/run/secrets/listener.edn:ro" \
    --volume "$scratch/listener.p12:/run/secrets/listener.p12:ro" \
    "$image" mirror --name "$name" --config /etc/datahike/server.edn >/dev/null
}

docker network create "$network" >/dev/null
docker run --detach --name "$publisher" --network "$network" --network-alias publisher \
  --env POSTGRES_PASSWORD=fixture-admin-password --volume "$publisher:/var/lib/postgresql" \
  "$publisher_image" -c wal_level=logical >/dev/null
await source_ready
source_sql <<SQL
CREATE SCHEMA source;
CREATE TABLE source.accounts (id integer PRIMARY KEY, note text);
CREATE TABLE source.events (id integer PRIMARY KEY);
INSERT INTO source.accounts VALUES (1, 'snapshot');
INSERT INTO source.events VALUES (1);
CREATE PUBLICATION analytics FOR TABLE source.accounts;
CREATE PUBLICATION metrics FOR TABLE source.events;
CREATE ROLE follower LOGIN REPLICATION PASSWORD '$publisher_password';
GRANT USAGE ON SCHEMA source TO follower;
GRANT SELECT ON ALL TABLES IN SCHEMA source TO follower;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_logical_emit_message(boolean, text, text, boolean) TO follower;
SQL

cat >"$scratch/server.edn" <<'EDN'
{:pg-listener {:host "127.0.0.1" :port 5432}
 :pg-mirrors
 {"analytics" {:publication "analytics" :store-path "/var/lib/datahike/mirror"
               :source-url-file "/run/secrets/analytics.url"
               :server-config-file "/run/secrets/listener.edn"
               :slot-prefix "pgdh_analytics" :schema-refresh? true
               :server {:host "0.0.0.0" :port 15432}}
  "metrics" {:publication "metrics" :store-path "/var/lib/datahike/mirror"
             :source-url-file "/run/secrets/metrics.url"
             :server-config-file "/run/secrets/listener.edn"
             :slot-prefix "pgdh_metrics" :server {:host "0.0.0.0" :port 15432}}}}
EDN
for name in analytics metrics; do
  printf 'jdbc:postgresql://publisher/postgres?user=follower&password=%s\n' "$publisher_password" >"$scratch/$name.url"
done
cat >"$scratch/listener.edn" <<EDN
{:users {"reader" "$reader_password"}
 :tls {:keystore "/run/secrets/listener.p12" :keystore-password "changeit"}}
EDN
keytool -genkeypair -noprompt -alias listener -storetype PKCS12 -keyalg RSA -keysize 2048 \
  -validity 2 -dname CN=localhost -ext SAN=dns:localhost -storepass changeit \
  -keystore "$scratch/listener.p12" >/dev/null 2>&1
keytool -exportcert -rfc -alias listener -keystore "$scratch/listener.p12" \
  -storepass changeit -file "$scratch/ca.crt" >/dev/null 2>&1
chmod 644 "$scratch/"*

docker run --detach --name "$native" --publish 127.0.0.1::4444 \
  --env "DATAHIKE_TOKEN=$token" --env 'JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2 -Xmx1g' \
  --volume "$native:/var/lib/datahike" --volume "$scratch/server.edn:/etc/datahike/server.edn:ro" \
  "$image" --config /etc/datahike/server.edn >/dev/null
start_mirror analytics "$analytics"
start_mirror metrics "$metrics"
analytics_port=$(published_port "$analytics" 15432)
metrics_port=$(published_port "$metrics" 15432)
http="http://127.0.0.1:$(published_port "$native" 4444)"
await curl --silent --fail --output /dev/null "$http/health/ready"
curl --silent --show-error --fail -H "authorization: token $token" "$http/version" >"$scratch/version"
if grep -q 'pg-mirrors' "$scratch/version"; then exit 1; fi
curl --silent --show-error --fail --output /dev/null \
  -H "authorization: token $token" -H 'content-type: application/edn' \
  --data-binary '[{:name "local" :store {:backend :file :id #uuid "ba04429a-9200-4928-9899-3722078dbe3f" :path "/var/lib/datahike/local"} :schema-flexibility :write}]' \
  "$http/create-database"
native_sql -c 'CREATE TABLE local_notes (id integer PRIMARY KEY); INSERT INTO local_notes VALUES (1);'
await mirror_result analytics "$analytics_port" 'SELECT note FROM source.accounts' snapshot
await mirror_result metrics "$metrics_port" 'SELECT count(*) FROM source.events' 1
test "$(mirror_sql analytics "$analytics_port" -c 'COPY source.accounts (note) TO STDOUT')" = snapshot
if mirror_sql analytics "$analytics_port" -c '\set VERBOSITY verbose' \
  -c "COPY source.accounts TO '/tmp/mirror-copy-denied'" >"$scratch/copy.log" 2>&1; then exit 1; fi
grep -q '25006' "$scratch/copy.log"
docker exec "$analytics" test ! -e /tmp/mirror-copy-denied
docker exec "$analytics" test ! -e /run/secrets/metrics.url
docker exec "$native" test ! -e /run/secrets/analytics.url
slot=$(source_sql -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pgdh_analytics_%'")
test -n "$slot"
echo 'Two mirrors and a writable local database are live with isolated mounts'

docker stop "$publisher" >/dev/null
await mirror_result analytics "$analytics_port" 'SELECT count(*) FROM source.accounts' 1
native_sql -c 'INSERT INTO local_notes VALUES (2)'
curl --silent --fail --output /dev/null "$http/health/ready"
docker start "$publisher" >/dev/null
await source_ready
source_sql -c "ALTER TABLE source.accounts ADD COLUMN score integer DEFAULT 5; INSERT INTO source.accounts VALUES (2, 'after outage', 8);"
source_sql -c 'INSERT INTO source.events VALUES (2)'
await mirror_result analytics "$analytics_port" 'SELECT sum(score) FROM source.accounts' 13
await mirror_result metrics "$metrics_port" 'SELECT count(*) FROM source.events' 2
echo 'Both workers caught up after an outage, including schema refresh'

mirror_sql analytics "$analytics_port" -c "SELECT 'reader ready'" -c 'SELECT pg_sleep(60)' >"$scratch/reader.log" 2>&1 &
reader_pid=$!
await grep -q 'reader ready' "$scratch/reader.log"
docker pause "$publisher" >/dev/null
# Let a slot poll enter the paused publisher before testing cancellation.
sleep 6
docker stop --time 15 "$analytics" >/dev/null
test "$(docker inspect -f '{{.State.ExitCode}}' "$analytics")" = 143
if wait "$reader_pid"; then exit 1; fi
docker unpause "$publisher" >/dev/null
source_sql -c "INSERT INTO source.accounts VALUES (3, 'after restart', 9)"
docker start "$analytics" >/dev/null
analytics_port=$(published_port "$analytics" 15432)
await mirror_result analytics "$analytics_port" 'SELECT sum(score) FROM source.accounts' 22
test "$(source_sql -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pgdh_analytics_%'")" = "$slot"
test "$(native_sql -c 'SELECT count(*) FROM local_notes')" = 2
echo 'SIGTERM drained an open reader during a publisher stall and restart reused the original slot'
