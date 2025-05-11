'use server';

import Database, { Database as DBT } from "better-sqlite3"
// import { Database } from 'sqlite3'
// import { open } from 'sqlite'

const DB = '/Users/ryan/.config/disk-tree/disk-tree.db'

let db: DBT | null = null;

function getDb() {
  if (!db) {
    // Consider adding error handling for database opening
    db = new Database(DB, { readonly: true });
    // Optional: Add a shutdown hook if running for a long time
    // process.on('exit', () => db?.close());
  }
  return db;
}

// const db = open({
//   filename: DB,
//   driver: Database,
// })

export async function getScans() {
  const db = getDb()
  const stmt = db.prepare('SELECT * FROM scan');
  const res = stmt.all();
  // const res = await (await db).all('SELECT * FROM scan')
  return res
  // return [ 1, 2, 3 ]
}
