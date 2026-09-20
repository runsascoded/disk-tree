// The help line's state (specs/edu-drawer.md §1): one explanation at a time,
// from whatever control is hovered or keyboard-focused. Pure — the provider
// in Help.tsx drives it from pointer/focus events.

import type { ReactNode } from 'react'

export interface HelpState {
  /** The hovered control's text (null = pointer over nothing explained). */
  hover: ReactNode | null
  /** The focused control's text (null = nothing explained has focus). */
  focus: ReactNode | null
}

export type HelpEvent =
  | { type: 'enter'; text: ReactNode }
  | { type: 'leave' }
  | { type: 'focus'; text: ReactNode }
  | { type: 'blur' }

export const HELP_EMPTY: HelpState = { hover: null, focus: null }

/** Hover and focus are tracked apart so leaving a control keeps a focused
 * one's text, and blurring falls back to whatever is still hovered. */
export function helpReducer(s: HelpState, e: HelpEvent): HelpState {
  switch (e.type) {
    case 'enter': return s.hover === e.text ? s : { ...s, hover: e.text }
    case 'leave': return s.hover === null ? s : { ...s, hover: null }
    case 'focus': return s.focus === e.text ? s : { ...s, focus: e.text }
    case 'blur': return s.focus === null ? s : { ...s, focus: null }
  }
}

/** What the line shows: the hovered control wins (it is where the pointer
 * is), else the focused one, else nothing. */
export const helpText = (s: HelpState): ReactNode | null => s.hover ?? s.focus
