# The m3 ingest image: a laptop capture (layer-1 listing shards in R2) →
# path index + snapshot, back to R2 (see ingest.sh, specs/m3-site.md).
#
# Built by Pulumi (`aws/__main__.py`) from THIS worktree — `m3` isn't pushed
# (it's a laptop deployment; `log.md` names local paths), so there's no
# public ref to clone. Context is the repo root; `Dockerfile.dockerignore`
# allowlists the Python sources. Both lockfiles are `--frozen`, as in
# `.github/workflows/daily-ingest.yml`: root venv = the `disk-tree` engine,
# `cloud/` venv = `dt-cloud`.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# GNU time: ingest.sh reports path-index's peak RSS (the spike's question).
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends time ca-certificates && rm -rf /var/lib/apt/lists/*
RUN uv tool install awscli==1.44.* && ln -s /root/.local/bin/aws /usr/local/bin/aws

WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
# The wheel force-includes the built UI; the ingest never serves it.
RUN mkdir -p ui/dist && uv sync --frozen --no-dev --extra r2
COPY cloud/pyproject.toml cloud/uv.lock ./cloud/
COPY cloud/src ./cloud/src
RUN cd cloud && uv sync --frozen --no-dev

COPY aws/ingest.sh /ingest.sh
ENTRYPOINT ["/ingest.sh"]
