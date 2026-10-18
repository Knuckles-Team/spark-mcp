# syntax=docker/dockerfile:1
# MCP serving image. It installs the published spark-mcp wheel from PyPI at the
# pinned version (tracked by .bumpversion.cfg), never the source tree, and
# builds no tools from source. pyspark ships only an sdist, so it is the one
# package exempted from --only-binary :all: (declared in pyproject.toml
# [tool.pipelines] allow-source-build).
#
# Python 3.12, not the fleet's 3.14: the Spark Connect client must match the
# 3.5.9 server, and pyspark 3.5's [connect] extra pins numpy<2, which publishes
# no CPython 3.14 wheel. Move to 3.14 when the server moves to Spark 4.
#
#   docker build --file docker/Dockerfile --target mcp -t spark-mcp:<version>-mcp .
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS mcp
ARG HOST=127.0.0.1
ARG PORT=8000
ARG TRANSPORT="stdio"
ARG AUTH_TYPE="none"
ENV HOST=${HOST} \
    PORT=${PORT} \
    TRANSPORT=${TRANSPORT} \
    AUTH_TYPE=${AUTH_TYPE} \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp \
    XDG_CONFIG_HOME=/tmp/.config \
    XDG_CACHE_HOME=/tmp/.cache
# Fixed identity for least-privilege runtime and stable volume ownership.
RUN pip install --only-binary :all: --no-binary pyspark spark-mcp[mcp]==0.3.0 \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --no-create-home \
        --home-dir /tmp --shell /usr/sbin/nologin app
USER 10001:10001
CMD ["spark-mcp"]

FROM mcp AS default
