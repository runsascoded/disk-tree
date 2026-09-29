-- Agents that check in with this deployment. The laptop drainer (`disk-tree
-- dispatch -s`) writes its heartbeat every poll, and the `laptop` executor
-- refuses a dispatch while that's stale — the items stay staged and the
-- console says why (specs/m3-site.md Phase 3). One row per agent name.
CREATE TABLE agents (
  name TEXT PRIMARY KEY,      -- `drainer`
  seen_ts INTEGER NOT NULL,   -- epoch seconds of the last check-in
  host TEXT                   -- the machine it runs on (informational)
);
