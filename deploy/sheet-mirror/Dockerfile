# Lightweight image for the scheduled sheet mirror (`sync.sh`): just the
# `dt-cloud` CLI (`export`, `sheet-push`, `sheet-mirror plan`) — no site build,
# no wrangler. Built from a minimal context `build.sh` stages (the repo-root
# layout, trimmed to what's copied here); see cloudbuild.yaml.
FROM python:3.12-slim
WORKDIR /app
# `dt-cloud` depends on the root `disk-tree` engine (a uv path source, which
# pip can't follow): install it from source first so the dependency is
# satisfied. Its wheel force-includes the built UI (ui/dist) — none is needed
# here, so an empty dir stands in.
COPY pyproject.toml README.md ./
COPY src ./src
RUN mkdir -p ui/dist && pip install --no-cache-dir .
COPY cloud/pyproject.toml ./cloud/
COPY cloud/src ./cloud/src
RUN pip install --no-cache-dir "./cloud[sheets]"
COPY deploy/sheet-mirror/sync.sh ./sync.sh
ENTRYPOINT ["bash", "sync.sh"]
