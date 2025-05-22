export type Kind = 'file' | 'dir'

export type Row = {
  path: string
  size: number
  mtime: number
  n_desc: number
  n_children: number
  kind: Kind
  parent: string | null
  uri: string
}

export type ScanDetails = {
  root: Row
  children: Row[]
  rows: Row[]
  time: string
}
