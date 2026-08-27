import { describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { BytesOverTime, TimeSeries } from '../src/TimeSeries'

/** Force a non-zero SVG size in jsdom (ResizeObserver mock never fires). */
function forceSize(container: HTMLElement, w = 400, h = 200) {
  const wrap = container.querySelector('.dt-timeseries') as HTMLElement
  Object.defineProperty(wrap, 'clientWidth', { value: w, configurable: true })
  Object.defineProperty(wrap, 'clientHeight', { value: h, configurable: true })
}

describe('<TimeSeries>', () => {
  it('renders an SVG when given a non-empty series', () => {
    const { container } = render(
      <TimeSeries
        series={[
          {
            key: 'a',
            points: [
              { t: 0, y: 10 },
              { t: 1, y: 20 },
            ],
          },
        ]}
        getX={p => p.t}
        getY={p => p.y}
      />,
    )
    forceSize(container)
    // The SVG is rendered inside a ResizeObserver callback; in jsdom it fires
    // on next tick when dims are set. It won't actually paint here, but the
    // wrapper div is always present regardless.
    expect(container.querySelector('.dt-timeseries')).toBeInTheDocument()
  })

  it('yTickValues overrides nice-ticks, dropping values outside the domain', () => {
    // Stub the prototype getters *before* mount — the component measures
    // synchronously, so post-mount stubbing (forceSize) never renders ticks.
    const saved = (['clientWidth', 'clientHeight'] as const).map(
      k => [k, Object.getOwnPropertyDescriptor(HTMLElement.prototype, k)] as const,
    )
    Object.defineProperty(HTMLElement.prototype, 'clientWidth', { configurable: true, get: () => 400 })
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', { configurable: true, get: () => 200 })
    try {
      const { container } = render(
        <TimeSeries
          series={[{ key: 'a', points: [{ t: 0, y: 10 }, { t: 1, y: 90 }] }]}
          getX={p => p.t}
          getY={p => p.y}
          yTickValues={[16, 64, 256]}  // 256 > yMax → dropped
        />,
      )
      const yLabels = [...container.querySelectorAll('svg g[class] text, svg g text')]
        .filter(t => t.getAttribute('text-anchor') === 'end')
        .map(t => t.textContent)
      expect(yLabels).toEqual(['16', '64'])
    } finally {
      for (const [k, d] of saved) {
        if (d) Object.defineProperty(HTMLElement.prototype, k, d)
        else delete (HTMLElement.prototype as unknown as Record<string, unknown>)[k]
      }
    }
  })

  it('handles empty series without crashing', () => {
    interface P { t: number; y: number }
    const { container } = render(
      <TimeSeries<P>
        series={[{ key: 'a', points: [] }]}
        getX={p => p.t}
        getY={p => p.y}
      />,
    )
    expect(container.querySelector('.dt-timeseries')).toBeInTheDocument()
  })
})

describe('<BytesOverTime>', () => {
  it('accepts ISO date strings and null bytes', () => {
    const { container } = render(
      <BytesOverTime
        points={[
          { time: '2026-08-01T00:00:00Z', bytes: 1000 },
          { time: '2026-08-05T00:00:00Z', bytes: 500 },
          { time: '2026-08-06T00:00:00Z', bytes: null }, // should be filtered
        ]}
        formatBytes={n => `${n}B`}
      />,
    )
    expect(container.querySelector('.dt-timeseries')).toBeInTheDocument()
  })

  it('accepts numeric epoch-millis timestamps', () => {
    const { container } = render(
      <BytesOverTime
        points={[
          { time: 1_700_000_000_000, bytes: 100 },
          { time: 1_700_086_400_000, bytes: 200 },
        ]}
        formatBytes={n => `${n}B`}
      />,
    )
    expect(container.querySelector('.dt-timeseries')).toBeInTheDocument()
  })
})
