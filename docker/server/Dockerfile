FROM docker.io/library/eclipse-temurin:21-jre-noble

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="Datahike server" \
      org.opencontainers.image.description="Batteries-included Datahike HTTP server" \
      org.opencontainers.image.url="https://datahike.io" \
      org.opencontainers.image.source="https://github.com/replikativ/datahike" \
      org.opencontainers.image.documentation="https://github.com/replikativ/datahike/blob/main/doc/distributed.md#http-server-setup" \
      org.opencontainers.image.licenses="EPL-1.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 datahike \
    && useradd --uid 10001 --gid 10001 --home-dir /opt/datahike \
       --no-create-home --shell /usr/sbin/nologin datahike \
    && mkdir --parents /opt/datahike /var/lib/datahike /run/datahike-nrepl \
    && chown --recursive 10001:10001 /opt/datahike /var/lib/datahike /run/datahike-nrepl

COPY --chown=10001:10001 datahike-http-server.jar /opt/datahike/datahike-http-server.jar

USER 10001:10001
WORKDIR /var/lib/datahike

ENV DATAHIKE_HOST="0.0.0.0" \
    DATAHIKE_PORT="4444" \
    DATAHIKE_SYSTEM_DB_PATH="/var/lib/datahike/system"

VOLUME ["/var/lib/datahike"]
EXPOSE 4444
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=6 \
  CMD curl --fail --silent --show-error \
      "http://127.0.0.1:${DATAHIKE_PORT:-4444}/health/live" >/dev/null || exit 1

ENTRYPOINT ["java", "-jar", "/opt/datahike/datahike-http-server.jar"]
