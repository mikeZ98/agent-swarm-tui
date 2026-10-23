# syntax=docker/dockerfile:1
#
# agent-swarm-tui: production image (an interactive terminal UI).
#
#   builder: resolves the locked production dependency set with uv and installs
#            the project as a regular (non-editable) wheel into /opt/venv.
#   runner:  python:3.13-slim + /opt/venv only. No uv, no pip, no sources, no
#            tests, no dev dependencies, no build caches. Runs as uid 10001 and
#            works on a read-only root filesystem: bytecode is precompiled at
#            build time and every runtime write (logs, screenshots) goes to /tmp.
#
# Run it with a TTY:  docker run --rm -it agent-swarm-tui:local [args...]
# Base images are pinned by digest (same pins as ai-inference-gateway); bump tag
# and digest together.

ARG PYTHON_IMAGE=python:3.13-slim@sha256:57b29a7c050c4e679e4bea572e14b3965cf4b5839de2da49d55e49ffc4024f39
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.7@sha256:240fb85ab0f263ef12f492d8476aa3a2e4e1e333f7d67fbdd923d00a506a516a

FROM ${UV_IMAGE} AS uv

# --------------------------------------------------------------------------- #
FROM ${PYTHON_IMAGE} AS builder

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_FROZEN=1

WORKDIR /build

# Dependencies first (cached until pyproject.toml / uv.lock change).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --no-dev --no-install-project

# Then the project itself, as a wheel (not editable): /build is not needed at runtime.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-editable

# --------------------------------------------------------------------------- #
FROM ${PYTHON_IMAGE} AS runner

LABEL org.opencontainers.image.title="agent-swarm-tui" \
      org.opencontainers.image.description="Textual TUI for orchestrating and observing an asyncio multi-agent swarm on ai-inference-gateway."

# The runtime never installs packages, so the base image's pip goes. Its
# vendored copies of urllib3, msgpack and setuptools carry HIGH CVEs that
# Trivy flags. The ensurepip wheels embedding the same copies go too. The app
# runs from /opt/venv, which uv builds without pip.
RUN groupadd --system --gid 10001 appuser \
 && useradd --system --uid 10001 --gid appuser --no-create-home \
            --home-dir /nonexistent --shell /usr/sbin/nologin appuser \
 && python -m pip uninstall --yes --no-input --root-user-action=ignore pip \
 && rm -rf /usr/local/lib/python3.13/ensurepip/_bundled

# Owned by root and not writable by appuser: the app cannot modify its own code.
COPY --from=builder --chown=root:root /opt/venv /opt/venv

# HOME/XDG/TMPDIR -> /tmp: the only writable path under `read_only: true`
# (Textual's "save screenshot" writes to the working directory or ~/Downloads).
# GATEWAY_BASE_URL defaults to the gateway's compose service name; 127.0.0.1
# would be the container itself.
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    LANG=C.UTF-8 \
    TERM=xterm-256color \
    HOME=/tmp \
    TMPDIR=/tmp \
    XDG_CACHE_HOME=/tmp/.cache \
    XDG_CONFIG_HOME=/tmp/.config \
    XDG_DATA_HOME=/tmp/.local/share \
    GATEWAY_BASE_URL=http://gateway:8000

WORKDIR /tmp
USER 10001:10001

# Interactive client: nothing to probe.
HEALTHCHECK NONE

ENTRYPOINT ["agent-swarm-tui"]
CMD ["--log-file", "/tmp/agent-swarm-tui.log"]
