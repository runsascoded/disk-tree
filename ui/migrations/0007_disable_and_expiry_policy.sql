-- Two verbs, not one. Revoking a link has always killed every session it ever
-- minted — that is the point of the per-request re-join — but "stop handing out
-- new sessions, leave the people already inside alone" had no trigger, even
-- though it is exactly what exhausting `max_redeems` already does.
--
-- `disabled_at` is that trigger: checked when redeeming, ignored when
-- re-joining. `revoked_at` keeps its meaning and is checked by both.
ALTER TABLE grants ADD COLUMN disabled_at INTEGER;

-- Whether the link's own expiry also ends the sessions derived from it.
--
-- These are genuinely different questions — a `max_redeems: 1` link stops being
-- redeemable the moment it is used, and nobody expects that to log the
-- recipient out. Expiry conflated them: 1 (the default) keeps today's
-- data-room reading, where "expires Friday" means access ends Friday; 0 makes
-- `expires_at` purely a redemption window, and the session then lives out its
-- own `session_ttl`.
ALTER TABLE grants ADD COLUMN expiry_ends_sessions INTEGER NOT NULL DEFAULT 1;

CREATE INDEX grants_disabled_at ON grants (disabled_at);
