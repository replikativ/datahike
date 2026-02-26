/**
 * Isomorphic test suite for Datahike JavaScript API.
 * Works in both Node.js and browser environments using memory backend.
 * 
 * Usage:
 * - Node.js: npx ts-node test-isomorphic.ts
 * - Browser: Loaded by karma test runner
 */

import * as d from './index';

export interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
  duration?: number;
}

function generateUUID(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export async function runIsomorphicTests(
  log: (msg: string) => void = console.log
): Promise<{ passed: number; failed: number; results: TestResult[] }> {
  const results: TestResult[] = [];
  let passed = 0;
  let failed = 0;

  async function runTest(name: string, testFn: () => Promise<void>): Promise<void> {
    const start = Date.now();
    try {
      await testFn();
      results.push({ name, passed: true, duration: Date.now() - start });
      passed++;
      log(`  ✓ ${name}`);
    } catch (err: any) {
      results.push({ name, passed: false, error: err.message, duration: Date.now() - start });
      failed++;
      log(`  ✗ ${name}: ${err.message}`);
    }
  }

  log('\n=== Isomorphic Datahike Tests (Memory Backend) ===\n');

  await runTest('Basic database operations', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const exists = await d.databaseExists(config);
    if (!exists) throw new Error('Database should exist');
    
    const conn = await d.connect(config);
    const db = await d.db(conn);
    if (!db) throw new Error('DB value should exist');
    
    d.release(conn);
    await d.deleteDatabase(config);
    
    const existsAfter = await d.databaseExists(config);
    if (existsAfter) throw new Error('Database should not exist after deletion');
  });

  await runTest('Schema and transactions', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
      { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
    ];
    
    const schemaTx = await d.transact(conn, schema);
    if (!schemaTx['tx-data'] || schemaTx['tx-data'].length === 0) {
      throw new Error('Schema transaction should have datoms');
    }
    
    const data = [
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 }
    ];
    const dataTx = await d.transact(conn, data);
    if (!dataTx['tx-data'] || dataTx['tx-data'].length === 0) {
      throw new Error('Data transaction should have datoms');
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await runTest('Datoms API', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    
    await d.transact(conn, [{ name: 'Alice' }, { name: 'Bob' }]);
    
    const db = await d.db(conn);
    const eavtDatoms = await d.datoms(db, ':eavt');
    if (!eavtDatoms || eavtDatoms.length === 0) {
      throw new Error('EAVT datoms should not be empty');
    }
    
    const avetDatoms = await d.datoms(db, ':avet');
    if (!avetDatoms || avetDatoms.length === 0) {
      throw new Error('AVET datoms should not be empty');
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await runTest('Pull API', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
      { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    await d.transact(conn, [{ name: 'Alice', age: 30 }]);
    
    const db = await d.db(conn);
    const queryResult = await d.q('[:find ?e :where [?e :name "Alice"]]', db);
    const eid = queryResult[0][0];
    
    const pulled = await d.pull(db, [':name', ':age'], eid);
    if (!pulled || pulled.name !== 'Alice') {
      throw new Error('Pull failed or incorrect data');
    }
    
    const pulledAll = await d.pull(db, ['*'], eid);
    if (!pulledAll || !pulledAll.name) {
      throw new Error('Pull with wildcard failed');
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await runTest('Query API', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
      { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    
    await d.transact(conn, [
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 }
    ]);
    
    const db = await d.db(conn);
    
    const q1 = await d.q('[:find ?e ?name :where [?e :name ?name]]', db);
    if (!q1 || q1.length !== 3) {
      throw new Error(`Expected 3 results, got ${q1 ? q1.length : 0}`);
    }
    
    const q2 = await d.q('[:find [?name ...] :where [_ :name ?name]]', db);
    if (!q2 || q2.length !== 3) {
      throw new Error(`Expected 3 names, got ${q2 ? q2.length : 0}`);
    }
    
    const q3 = await d.q('[:find ?name . :where [?e :name ?name] [?e :age 25]]', db);
    if (q3 !== 'Bob') {
      throw new Error(`Expected "Bob", got ${q3}`);
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await runTest('Temporal features (history, as-of)', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() },
      'keep-history?': true
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':value', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    
    await d.transact(conn, [{ value: 10 }]);
    const db1 = await d.db(conn);
    
    await d.transact(conn, [{ value: 20 }]);
    const db2 = await d.db(conn);
    
    const histDb = await d.history(db2);
    const histDatoms = await d.datoms(histDb, ':eavt');
    if (!histDatoms || histDatoms.length < 2) {
      throw new Error('History should have multiple datoms');
    }
    
    const dbAsOf = await d.asOf(db2, db1);
    const asOfDatoms = await d.datoms(dbAsOf, ':eavt');
    if (!asOfDatoms || asOfDatoms.length === 0) {
      throw new Error('as-of should return datoms');
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await runTest('Schema retrieval', async () => {
    const config = {
      store: { backend: ':memory', id: generateUUID() }
    };
    
    await d.createDatabase(config);
    const conn = await d.connect(config);
    
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
      { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    
    const db = await d.db(conn);
    const retrievedSchema = await d.schema(db);
    
    if (!retrievedSchema || Object.keys(retrievedSchema).length === 0) {
      throw new Error('Schema retrieval failed');
    }
    
    d.release(conn);
    await d.deleteDatabase(config);
  });

  log('\n=== Test Summary ===');
  log(`Passed: ${passed}`);
  log(`Failed: ${failed}`);
  log(`Total: ${passed + failed}`);

  return { passed, failed, results };
}

if (typeof require !== 'undefined' && require.main === module) {
  runIsomorphicTests().then(({ passed, failed }) => {
    setTimeout(() => process.exit(failed > 0 ? 1 : 0), 1000);
  });
}