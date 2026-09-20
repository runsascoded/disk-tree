-- Grant rotate (package migration 0011): `gate.rotate(id, { endSessions })`
-- re-keys a leaked share link (new token, same grant row). With `endSessions`,
-- rotate also boots sessions already derived from the old link — enforced by
-- stamping this epoch and rejecting any session whose `iat` predates it on the
-- per-request re-join. NULL (the default) = re-key only, live sessions untouched.
ALTER TABLE grants ADD COLUMN sessions_invalid_before INTEGER;
