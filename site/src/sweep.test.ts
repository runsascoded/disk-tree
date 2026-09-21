import { describe, expect, it } from 'vitest'
import { looksCkpt } from './sweep'
import type { TreeNode } from './types'

describe('looksCkpt within ~2 levels', () => {
  const step = (n: string) => ({ n, b: 1, o: 1 } as TreeNode)
  const run = { n: 'run-7', b: 1, o: 1, c: [step('step-100'), step('step-200')] } as TreeNode
  const runGroup = { n: 'checkpoints', b: 1, o: 1, c: [run] } as TreeNode  // steps 2 levels below
  const marin = { n: 'marin', b: 1, o: 1, c: [runGroup] } as TreeNode  // steps 3 levels below
  it('offers at a run dir (steps one level down)', () => {
    expect(looksCkpt(run)).toBe(true)
  })
  it('offers at the run\'s parent (steps two levels down)', () => {
    expect(looksCkpt(runGroup)).toBe(true)
  })
  it('does not offer higher up (steps 3+ levels down)', () => {
    expect(looksCkpt(marin)).toBe(false)
  })
  it('a leaf with no step-numbered children is not offerable', () => {
    expect(looksCkpt(step('step-100.safetensors'))).toBe(false)
  })
})
