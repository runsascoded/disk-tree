import { createContext, useCallback, useContext, useEffect, useId, useMemo, useReducer, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { MdHelpOutline } from 'react-icons/md'
import { HELP_EMPTY, helpReducer, helpText } from './helpState'
import type { HelpEvent } from './helpState'
import { useHelpPref } from './prefs'

// The help line (specs/edu-drawer.md): explanations of controls leave the
// floating layer for one fixed strip at the bottom of the viewport, which
// mirrors whatever control is hovered or keyboard-focused and is empty
// otherwise. Data tips (cells, rows) stay `<Tooltip>`s.

const HelpCtx = createContext<{ text: ReactNode | null; send: (e: HelpEvent) => void } | null>(null)

export function HelpProvider({ children }: { children: ReactNode }) {
  const [state, send] = useReducer(helpReducer, HELP_EMPTY)
  const value = useMemo(() => ({ text: helpText(state), send }), [state])
  return <HelpCtx.Provider value={value}>{children}</HelpCtx.Provider>
}

/** Wrap a control: its explanation shows in the help line while the control
 * is hovered or focused, and is always available to assistive tech through
 * `aria-describedby`. Nothing floats. */
export function Explain({ text, children, className }: { text: ReactNode; children: ReactNode; className?: string }) {
  const ctx = useContext(HelpCtx)
  const [on] = useHelpPref()
  const id = useId()
  const send = ctx?.send
  const enter = useCallback(() => send?.({ type: 'enter', text }), [send, text])
  const leave = useCallback(() => send?.({ type: 'leave' }), [send])
  const focus = useCallback(() => send?.({ type: 'focus', text }), [send, text])
  const blur = useCallback(() => send?.({ type: 'blur' }), [send])
  const live = on === 'on' && !!send
  return (
    <span
      className={`has-help${className ? ` ${className}` : ''}`}
      aria-describedby={id}
      onPointerEnter={live ? enter : undefined}
      onPointerLeave={live ? leave : undefined}
      onFocus={live ? focus : undefined}
      onBlur={live ? blur : undefined}
    >
      {children}
      <span id={id} className="sr-only">{text}</span>
    </span>
  )
}

const HELP_INTRO = (
  <>Hover (or keyboard-focus) any control and its explanation shows here.
  Press <kbd>h</kbd> to turn this off.</>
)

/** The help card (specs/edu-drawer.md): a bottom-left panel that shows the
 * hovered/focused control's explanation. Idle it collapses to a small "?"
 * chip; clicking the chip OPENS the card with a short intro (so a click does
 * something, rather than the old behaviour of silently turning help off), and
 * the card's × collapses back to the chip. `h` / the SpeedDial toggle help
 * off entirely. The card lingers ~1s after the last control so a glance at it
 * isn't a race. */
export function HelpCard() {
  const ctx = useContext(HelpCtx)
  const [on] = useHelpPref()
  const [intro, setIntro] = useState(false)
  const live = ctx?.text ?? null
  // Hold the last text briefly after the pointer leaves, so reading the card
  // doesn't clear it (moving off the control to the card would otherwise blank
  // it). A fresh text cancels the pending clear.
  const [shown, setShown] = useState<ReactNode | null>(null)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => {
    if (timer.current) clearTimeout(timer.current)
    if (live != null) { setShown(live); return }
    timer.current = setTimeout(() => setShown(null), 1000)
    return () => { if (timer.current) clearTimeout(timer.current) }
  }, [live])
  if (on !== 'on') return null
  // What the card shows: a hovered control's text, else the intro if it was
  // opened from the chip, else nothing (collapsed to the chip).
  const body = shown ?? (intro ? HELP_INTRO : null)
  if (body == null) {
    return (
      <button type="button" className="help-chip" onClick={() => setIntro(true)}
        title="What do these controls do?" aria-label="Show help">
        <MdHelpOutline aria-hidden /> help
      </button>
    )
  }
  return (
    <aside className="help-card" role="status" aria-live="polite">
      <div className="head"><MdHelpOutline aria-hidden /> <span>what this does</span>
        <button type="button" className="x" onClick={() => { setIntro(false); setShown(null) }} title="Collapse to the help chip" aria-label="Collapse help card">×</button>
      </div>
      <div className="body">{body}</div>
    </aside>
  )
}
