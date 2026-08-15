# syntax=docker/dockerfile:1

# Single image, four supervised processes (spec §3.2, §10): log-listener,
# logstash, redis, log-server under s6-overlay. See supervisor/s6-rc.d for
# the service definitions and dependency ordering.

#############################################
# Stage 1: build the Python venv via uv
#############################################
FROM python:3.12-slim AS python-builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml uv.lock ./
COPY src src
# --no-editable: log-sump gets installed as a real package, not a path
# reference back into /build -- the venv is self-contained and doesn't need
# the source tree alongside it at runtime.
RUN uv sync --frozen --no-dev --no-editable

#############################################
# Stage 2: runtime
#############################################
FROM python:3.12-slim AS runtime

ARG TARGETARCH
ARG S6_OVERLAY_VERSION=3.2.3.2
ARG LOGSTASH_VERSION=8.15.3
ARG DOCKER_CLI_VERSION=27.3.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        redis-server \
        openssh-client \
        ca-certificates \
        xz-utils \
        curl \
    && rm -rf /var/lib/apt/lists/*

# docker CLI only (not the full engine -- this container never runs its own
# dockerd): LocalTransport (transport.py) shells out to a plain `docker`
# binary with no daemon-connection flags at all, so for the embedded "This
# machine" gateway target to see the *host's* containers, this image needs
# the CLI on PATH talking to the host's daemon over a mounted
# /var/run/docker.sock (see releases/_shared/docker-compose.yml in the
# parent cttc repo). The static binary (not an apt package) avoids adding
# Docker's own apt repo/signing key just for one binary -- same approach,
# same pinned version, as cttc's own now-superseded app/server/Dockerfile.
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) DOCKER_ARCH=x86_64 ;; \
        arm64) DOCKER_ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" \
        | tar -xz --strip-components=1 -C /usr/local/bin docker/docker; \
    docker --version

# `ssh <user>@<host> docker ...` (SSHTransport) execs plain ssh with no
# flags of its own, so without this it either hangs on an interactive
# host-key prompt (no tty/stdin available to a subprocess) or fails outright
# on first contact with a daemon just added at runtime (POST /daemons).
# accept-new is TOFU-on-first-connection, same trust model most people
# already accept from an interactive `ssh` the first time; it still detects
# a *changed* key later, unlike StrictHostKeyChecking no/UserKnownHostsFile=
# /dev/null. Container filesystem is ephemeral, so this effectively
# re-TOFUs every container restart -- acceptable since restarts are an
# operator action, not something reachable by whatever's on the other end
# of the ssh connection. Matches app/server/Dockerfile's identical setup.
RUN mkdir -p /etc/ssh/ssh_config.d && printf 'Host *\n  StrictHostKeyChecking accept-new\n' \
    > /etc/ssh/ssh_config.d/99-cttc-accept-new.conf

# --- s6-overlay (process supervisor, spec §3.2) ---
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) S6_ARCH=x86_64 ;; \
        arm64) S6_ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/s6-overlay-noarch.tar.xz \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz"; \
    tar -C / -Jxpf /tmp/s6-overlay-noarch.tar.xz; \
    curl -fsSL -o /tmp/s6-overlay-arch.tar.xz \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${S6_ARCH}.tar.xz"; \
    tar -C / -Jxpf /tmp/s6-overlay-arch.tar.xz; \
    rm -f /tmp/s6-overlay-*.tar.xz

# --- Logstash: official tarball, not the Docker image -- bundles its own
# JDK (no system Java needed), and downloading it directly avoids either
# cross-distro binary-copy risk (this base is Debian; the official Logstash
# image is Ubuntu) or the complexity of adding Elastic's apt repo/signing
# key just for one package. ---
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) LS_ARCH=x86_64 ;; \
        arm64) LS_ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/logstash.tar.gz \
        "https://artifacts.elastic.co/downloads/logstash/logstash-${LOGSTASH_VERSION}-linux-${LS_ARCH}.tar.gz"; \
    mkdir -p /usr/share/logstash; \
    tar -xzf /tmp/logstash.tar.gz -C /usr/share/logstash --strip-components=1; \
    rm -f /tmp/logstash.tar.gz
ENV PATH="/usr/share/logstash/bin:${PATH}"
COPY logstash/logstash.yml /usr/share/logstash/config/logstash.yml
COPY logstash/pipelines.yml /usr/share/logstash/config/pipelines.yml
COPY logstash/pipeline /usr/share/logstash/pipeline

# --- the Python venv built in stage 1 ---
COPY --from=python-builder /build/.venv /opt/log-sump/.venv
ENV PATH="/opt/log-sump/.venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

# --- service supervision ---
COPY supervisor/s6-rc.d /etc/s6-overlay/s6-rc.d

RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 8080

# ${LOG_SUMP_SERVER__BIND_PORT:-8080}, not a bare 8080: a deployment that
# overrides the bind port (see config.py's env_prefix) still needs this to
# probe the port log-server is actually listening on -- confirmed the hard
# way against a real 8765 override, without this HEALTHCHECK always
# reported unhealthy despite the app itself being up and correctly bound.
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://127.0.0.1:${LOG_SUMP_SERVER__BIND_PORT:-8080}/health/live || exit 1

ENTRYPOINT ["/init"]
