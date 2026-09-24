-- @open-athena/auth package migration 0008 (`migrations/0008_profiles.sql` at the
-- pinned dist), verbatim below. cw-s3 adopts the package's session model —
-- grants, access log, requests, pending email-code auth — for the ZT-free
-- sign-in (specs/oidc-cutover-cw.md); gcs carries the same tables in its own
-- lineage.

-- Self-set identity for a signed-in principal: the name and face an SSO user
-- chooses for themselves, which `grants.subject_json` never modelled because a
-- grant's subject is admin-owned (the anti-forwarding signal). Keyed by email
-- because that is the one identity an SSO id_token and a magic-link grant share,
-- and it is stable across sessions and devices — sign in tomorrow elsewhere and
-- your face follows you.
--
-- `avatar` is a `data:` URI in the common case (the same inlining
-- `core/avatar.ts` already does for grants, ≤ 64 KB, zero new infra) or an
-- `asset://<id>` ref served from the app's own origin when an `AssetStore` is
-- bound. It is never a live remote URL: a third-party `<img src>` on a private
-- page leaks "this person opened this" to that host on every render.
CREATE TABLE profiles (
  email       TEXT PRIMARY KEY,
  first       TEXT,
  last        TEXT,
  avatar      TEXT,
  -- Provenance for re-resolve/debug: 'upload' | 'url' | 'github' | 'gravatar'.
  avatar_src  TEXT,
  updated_at  INTEGER NOT NULL
);
