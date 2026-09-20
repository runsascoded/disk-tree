import { useState } from 'react'

// User avatars. We show a real photo ONLY when we have an explicit GitHub handle
// (github.com/<handle>.png) — curated in identities.yaml. We deliberately do NOT
// guess the handle from the email local-part: a guess that resolves to a
// different person's GitHub would paint the wrong face. No handle → colored
// initial. See identities.gen.ts / specs/avatar-sources.md.

export const avatarHue = (s: string): number => {
  let h = 0
  for (const c of s) h = (h * 31 + c.codePointAt(0)!) % 360
  return h
}

/**
 * Email or display string → canonical-id-shaped slug: the local part, sanitized
 * like rigging's `sanitize_username` (lowercase; runs of non-`[a-z0-9_-]` → '-').
 * So `will.held@openathena.ai` → `will-held` = the canonical id (the registry key).
 */
export const whoToHandle = (who: string): string =>
  who.split('@')[0].toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '')

export function Avatar({ github, name, size = 20 }: { github?: string; name: string; size?: number }) {
  const [failed, setFailed] = useState(false)
  const label = name.trim()
  const initial = label ? label[0].toUpperCase() : '?'
  if (failed || !github) {
    return (
      <span
        className="user-avatar fallback"
        style={{ width: size, height: size, fontSize: Math.round(size * 0.48), background: `hsl(${avatarHue(label)} 55% 42%)` }}
        aria-hidden
      >
        {initial}
      </span>
    )
  }
  return (
    <img
      className="user-avatar"
      src={`https://github.com/${github}.png?size=${size * 2}`}
      width={size}
      height={size}
      alt={label}
      loading="lazy"
      onError={() => setFailed(true)}
    />
  )
}
