/**
 * Browser test entry point for Karma.
 * Tests the browser build of Datahike JS API using memory backend.
 */

const testResults = { passed: 0, failed: 0 };

function log(msg) {
  console.log(msg);
  const output = document.getElementById('test-output');
  if (output) output.textContent += msg + '\n';
}

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

async function runTests() {
  log('\n=== Browser Datahike Tests (Memory Backend) ===\n');

  const d = window.datahike?.js?.api;
  if (!d) {
    log('ERROR: datahike.js.api not found on window');
    return false;
  }

  async function test(name, fn) {
    try {
      await fn();
      testResults.passed++;
      log('  ✓ ' + name);
      return true;
    } catch (err) {
      testResults.failed++;
      log('  ✗ ' + name + ': ' + err.message);
      return false;
    }
  }

  await test('Basic database operations', async () => {
    const config = { store: { backend: ':memory', id: generateUUID() } };
    await d.createDatabase(config);
    const exists = await d.databaseExists(config);
    if (!exists) throw new Error('Database should exist');
    const conn = await d.connect(config);
    const db = await d.db(conn);
    if (!db) throw new Error('DB should exist');
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await test('Schema and transactions', async () => {
    const config = { store: { backend: ':memory', id: generateUUID() } };
    await d.createDatabase(config);
    const conn = await d.connect(config);
    const schema = [
      { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
    ];
    await d.transact(conn, schema);
    await d.transact(conn, [{ name: 'Alice' }]);
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await test('Query API', async () => {
    const config = { store: { backend: ':memory', id: generateUUID() } };
    await d.createDatabase(config);
    const conn = await d.connect(config);
    await d.transact(conn, [{ 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }]);
    await d.transact(conn, [{ name: 'Alice' }, { name: 'Bob' }]);
    const db = await d.db(conn);
    const results = await d.q('[:find ?n :where [_ :name ?n]]', db);
    if (results.length !== 2) throw new Error('Expected 2 results');
    d.release(conn);
    await d.deleteDatabase(config);
  });

  await test('Pull API', async () => {
    const config = { store: { backend: ':memory', id: generateUUID() } };
    await d.createDatabase(config);
    const conn = await d.connect(config);
    await d.transact(conn, [{ 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }]);
    await d.transact(conn, [{ name: 'Alice' }]);
    const db = await d.db(conn);
    const queryResult = await d.q('[:find ?e :where [?e :name "Alice"]]', db);
    const eid = queryResult[0][0];
    const pulled = await d.pull(db, [':name'], eid);
    if (pulled.name !== 'Alice') throw new Error('Pull failed');
    d.release(conn);
    await d.deleteDatabase(config);
  });

  log('\n=== Test Summary ===');
  log('Passed: ' + testResults.passed);
  log('Failed: ' + testResults.failed);

  return testResults.failed === 0;
}

if (typeof window !== 'undefined') {
  window.runBrowserTests = runTests;
}

export { runTests };