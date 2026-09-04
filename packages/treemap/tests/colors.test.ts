import { describe, expect, it } from 'vitest'
import { contrastEdge, parseColor } from '../src/colors'

describe('parseColor', () => {
  it('parses #rgb / #rrggbb / #rrggbbaa', () => {
    expect(parseColor('#fff')).toEqual([255, 255, 255, 1])
    expect(parseColor('#204')).toEqual([34, 0, 68, 1])
    expect(parseColor('#20242a')).toEqual([32, 36, 42, 1])
    expect(parseColor('#20242a80')).toEqual([32, 36, 42, 128 / 255])
  })

  it('parses rgb() and rgba(), comma- or space-separated', () => {
    expect(parseColor('rgb(10, 20, 30)')).toEqual([10, 20, 30, 1])
    expect(parseColor('rgba(10, 20, 30, 0.5)')).toEqual([10, 20, 30, 0.5])
    expect(parseColor('rgb(10 20 30 / 0.25)')).toEqual([10, 20, 30, 0.25])
  })

  it('parses hsl()/hsla(), including the default palette form', () => {
    // hsl(0 0% 0%) is black, hsl(0 0% 100%) white.
    expect(parseColor('hsl(0 0% 0%)')).toEqual([0, 0, 0, 1])
    expect(parseColor('hsl(0, 0%, 100%)')).toEqual([255, 255, 255, 1])
    // Blue palette slot: hue 210, 70% sat, 55% light.
    expect(parseColor('hsl(210 70% 55%)')).toEqual([60, 140, 221, 1])
    expect(parseColor('hsla(210 70% 55% / 0.4)')).toEqual([60, 140, 221, 0.4])
  })

  it('returns null for colors it cannot parse', () => {
    expect(parseColor('var(--dt-treemap-container-bg, #202024)')).toBeNull()
    expect(parseColor('color-mix(in oklch, red, blue 40%)')).toBeNull()
    expect(parseColor('rebeccapurple')).toBeNull()
    expect(parseColor('linear-gradient(45deg, red, blue)')).toBeNull()
  })
})

describe('contrastEdge', () => {
  it('returns a dark stroke on a light face, light on a dark face', () => {
    expect(contrastEdge('#ffffff')).toBe('rgba(0, 0, 0, 0.55)')
    expect(contrastEdge('#111111')).toBe('rgba(255, 255, 255, 0.42)')
  })

  it('composites over the base at the given fade before deciding', () => {
    // A light face faded hard toward the dark base reads dark → light stroke.
    expect(contrastEdge('#ffffff', 0.1)).toBe('rgba(255, 255, 255, 0.42)')
    // …but unfaded it stays light → dark stroke.
    expect(contrastEdge('#ffffff', 1)).toBe('rgba(0, 0, 0, 0.55)')
  })

  it('returns null for an unparseable face so the caller keeps its fallback', () => {
    expect(contrastEdge('var(--dt-treemap-container-bg, #202024)')).toBeNull()
    expect(contrastEdge(undefined)).toBeNull()
  })
})
