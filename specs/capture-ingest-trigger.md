# Ingest new captures from the cloud side

**From:** the `tauri-native-app` session (`wt/app`), 2026-10-01. Part of `rust-engine.md` phase 3; Ryan chose this option over keeping the submit on the laptop ("latter sg").

## Why

disky now captures in-process: the scan job's `to` writes the layer-1 capture (`r2://disk-tree/captures/<host>/<root slug>/<stamp>/` + `_SUCCESS.json`) from the app binary, with no Python. What's left on the laptop is `aws/submit`: it runs as disky's `then` step, with `AWS_PROFILE=r` (RAC AWS keys on the laptop). For anyone but Ryan, the laptop should hold an R2 write credential and nothing else. The ingest should start because a capture landed.

## Shape (recommended)

1. **R2 event notification** on bucket `disk-tree`: object-create, prefix `captures/`, suffix `_SUCCESS.json` → a Cloudflare Queue (`disk-tree-captures`). The manifest is written last, so its arrival means the capture is complete.
2. **Consumer Worker** (small, its own `wrangler.toml`; or Pulumi under `cf/` if that's where m3's CF IaC lives):
   - Per message, derive the capture dir (the key minus `/_SUCCESS.json`).
   - Skip if `<capture dir>/_INGEST.json` exists, through the R2 binding. Queues deliver at least once.
   - Call Batch `SubmitJob` (`disk-tree-m3` / `disk-tree-m3-ingest`, `containerOverrides.command = ["r2://disk-tree/<capture dir>"]`), signed with `aws4fetch`.
   - Write `_INGEST.json` `{job_id, submitted}`.
   - On error, retry the message.
3. **IAM:** a dedicated user/key for the Worker, allowed only `batch:SubmitJob` on that queue + job definition ARNs. Store it as Worker secrets. Add it to `aws/__main__.py`.

Alternative, if a CF→AWS key is unwelcome: an EventBridge-scheduled Lambda (every 15 min) that lists `captures/**/_SUCCESS.json` without an `_INGEST.json` and submits. That needs R2 read creds in AWS instead, and adds up to 15 min of latency.

## Also

- `ingest.sh` hard-codes `STORE=laptop`. With more than one host, derive the store from the capture's `<host>` segment, or keep a host → store map. Today only `m3` captures.
- Make the ingest idempotent per capture, not just per submit. A re-sent message must not double-write an index gen.

## Cutover

1. Once the trigger is live and has ingested one disky capture end to end, drop `then` from the scan job in `~/.config/disk-tree/disky.json`.
2. Keep `aws/submit` for manual re-ingests.
3. Until then, disky's `then` keeps submitting, and both paths can't run at once (no trigger yet).

## disky's scan job config (for reference)

```json
"scan": {"to": "r2://disk-tree/captures", "host": "m3", "log": "index",
         "env": {"AWS_PROFILE": "m3", "DISK_TREE_R2_ENDPOINT_URL": "https://…r2.cloudflarestorage.com"},
         "then": {"command": ["/Users/ryan/c/disky/wt/m3/.venv/bin/python", "/Users/ryan/c/disky/wt/m3/aws/submit"],
                  "env": {"AWS_PROFILE": "r"}}}
```

`aws/laptop-scan` is superseded by this config, and can go once the switch is verified.
