// Comprehensive test for reorganized JS API - covers all nodejs_test.cljs functionality
const d = require('./datahike.js.api.js');
const fs = require('fs');
const os = require('os');
const path = require('path');

// Helper to find entity ID by name
async function findEntityByName(db, name) {
  const datoms = await d.datoms(db, ':eavt');
  if (!datoms) return null;
  
  // Find datom where attribute is :name and value matches
  // Use String(datom.a) since ClojureScript keyword internals get renamed by advanced compilation
  for (const datom of datoms) {
    if (datom.a && String(datom.a) === ':name' && datom.v === name) {
      return datom.e;
    }
  }
  return null;
}

// Helper to create temp directory
function tmpDir() {
  return path.join(os.tmpdir(), `datahike-js-test-${Date.now()}-${Math.floor(Math.random() * 10000)}`);
}

async function testBasicOperations() {
  console.log('\n=== Test 1: Basic Database Operations ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  console.log('  Creating database...');
  await d.createDatabase(config);
  
  console.log('  Checking existence...');
  const exists = await d.databaseExists(config);
  if (!exists) throw new Error('Database should exist');
  console.log('  ✓ Database exists');
  
  console.log('  Connecting...');
  const conn = await d.connect(config);
  console.log('  ✓ Connected');
  
  console.log('  Getting DB value...');
  const db = await d.db(conn);
  if (!db) throw new Error('DB value should exist');
  console.log('  ✓ DB value retrieved');
  
  d.release(conn);
  await d.deleteDatabase(config);
  
  const existsAfter = await d.databaseExists(config);
  if (existsAfter) throw new Error('Database should not exist after deletion');
  console.log('  ✓ Database deleted');
}

async function testSchemaAndTransactions() {
  console.log('\n=== Test 2: Schema and Transactions ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  console.log('  Transacting schema...');
  const schema = [
    {
      'db/ident': ':name',
      'db/valueType': ':db.type/string',
      'db/cardinality': ':db.cardinality/one'
    },
    {
      'db/ident': ':age',
      'db/valueType': ':db.type/long',
      'db/cardinality': ':db.cardinality/one'
    },
    {
      'db/ident': ':email',
      'db/valueType': ':db.type/string',
      'db/cardinality': ':db.cardinality/one'
    }
  ];
  
  const schemaTx = await d.transact(conn, schema);
  if (!schemaTx['tx-data'] || schemaTx['tx-data'].length === 0) {
    throw new Error('Schema transaction should have datoms');
  }
  console.log(`  ✓ Schema transacted (${schemaTx['tx-data'].length} datoms)`);
  
  console.log('  Transacting data...');
  const data = [
    { name: 'Alice', age: 30, email: 'alice@example.com' },
    { name: 'Bob', age: 25, email: 'bob@example.com' },
    { name: 'Charlie', age: 35, email: 'charlie@example.com' }
  ];
  
  const dataTx = await d.transact(conn, data);
  console.log(`  ✓ Data transacted (${dataTx['tx-data'].length} datoms)`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testDatomsAPI() {
  console.log('\n=== Test 3: Datoms API ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  const data = [
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 }
  ];
  await d.transact(conn, data);
  
  console.log('  Getting datoms from EAVT index...');
  const db = await d.db(conn);
  const eavtDatoms = await d.datoms(db, ':eavt');
  console.log(`  ✓ EAVT datoms: ${eavtDatoms.length}`);
  
  console.log('  Getting datoms from AVET index...');
  const avetDatoms = await d.datoms(db, ':avet');
  console.log(`  ✓ AVET datoms: ${avetDatoms.length}`);
  
  console.log('  Finding name datoms from EAVT...');
  const eavtForNames = await d.datoms(db, ':eavt');
  const nameDatoms = eavtForNames.filter(d => d.a && String(d.a) === ':name');
  if (nameDatoms.length !== 2) {
    throw new Error(`Expected 2 name datoms, got ${nameDatoms.length}`);
  }
  console.log(`  ✓ Name datoms: ${nameDatoms.length}`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testPullAPI() {
  console.log('\n=== Test 4: Pull API ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':email', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  const data = [
    { name: 'Alice', age: 30, email: 'alice@example.com' }
  ];
  await d.transact(conn, data);
  
  console.log('  Finding entity ID...');
  const db = await d.db(conn);
  const eid = await findEntityByName(db, 'Alice');
  if (!eid) throw new Error('Entity not found');
  console.log(`  ✓ Found entity: ${eid}`);
  
  console.log('  Pulling entity with pattern...');
  const pulled = await d.pull(db, [':name', ':age', ':email'], eid);
  if (!pulled || !pulled.name || pulled.name !== 'Alice') {
    throw new Error('Pull failed or incorrect data');
  }
  console.log(`  ✓ Pulled: ${pulled.name}, age ${pulled.age}, email ${pulled.email}`);
  
  console.log('  Pulling with wildcard...');
  const pulledAll = await d.pull(db, ['*'], eid);
  if (!pulledAll || !pulledAll.name) {
    throw new Error('Pull with wildcard failed');
  }
  console.log(`  ✓ Pulled with wildcard: ${Object.keys(pulledAll).length} keys`);
  
  console.log('  Pull many...');
  const pulledMany = await d.pullMany(db, [':name'], [eid]);
  if (!pulledMany || pulledMany.length !== 1) {
    throw new Error('Pull many failed');
  }
  console.log(`  ✓ Pull many: ${pulledMany.length} entities`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testEntityAPI() {
  console.log('\n=== Test 5: Entity API ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':email', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  const data = [
    { name: 'Bob', age: 25, email: 'bob@example.com' }
  ];
  await d.transact(conn, data);
  
  console.log('  Finding entity ID...');
  const db = await d.db(conn);
  const eid = await findEntityByName(db, 'Bob');
  if (!eid) throw new Error('Entity not found');
  console.log(`  ✓ Found entity: ${eid}`);
  
  console.log('  Getting entity...');
  const entity = await d.entity(db, eid);
  if (!entity) throw new Error('Entity retrieval failed');
  console.log(`  ✓ Entity retrieved (type: ${typeof entity})`);
  
  // Note: Entity API returns a ClojureScript Entity object, not a plain JS object
  // Use Pull API for getting entity data as plain JS object
  console.log('  Using pull to get entity data...');
  const pulled = await d.pull(db, [':name'], eid);
  if (pulled.name !== 'Bob') {
    throw new Error(`Expected name 'Bob' via pull, got '${pulled.name}'`);
  }
  console.log(`  ✓ Entity has correct name via pull: ${pulled.name}`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testTemporalDatabases() {
  console.log('\n=== Test 6: Temporal Databases (History, as-of) ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() },
    'keep-history?': true
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':value', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  console.log('  Transacting initial value...');
  await d.transact(conn, [{ value: 10 }]);
  const db1 = await d.db(conn);
  console.log('  ✓ Initial transaction complete');
  
  console.log('  Transacting updated value...');
  await d.transact(conn, [{ value: 20 }]);
  const db2 = await d.db(conn);
  console.log('  ✓ Updated transaction complete');
  
  console.log('  Checking current datoms...');
  const currentDatoms = await d.datoms(db2, ':eavt');
  const valueDatoms2 = currentDatoms.filter(d => d.a && d.a.fqn === 'value');
  const values2 = valueDatoms2.map(d => d.v);
  console.log(`  ✓ Current values in DB: ${values2.join(', ')}`);
  
  console.log('  Using as-of to view old state...');
  const dbAsOf = await d.asOf(db2, db1);
  const asOfDatoms = await d.datoms(dbAsOf, ':eavt');
  const valueDatoms1 = asOfDatoms.filter(d => d.a && d.a.fqn === 'value');
  const values1 = valueDatoms1.map(d => d.v);
  console.log(`  ✓ as-of values: ${values1.join(', ')}`);
  
  console.log('  Getting history database...');
  const histDb = await d.history(db2);
  const histDatoms = await d.datoms(histDb, ':eavt');
  if (histDatoms.length < 2) {
    throw new Error('History should have multiple datoms');
  }
  console.log(`  ✓ History DB has ${histDatoms.length} datoms`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testVersioningAndGc() {
  console.log('\n=== Test 7: Versioning and Garbage Collection ===');

  const config = {
    // GC operates on persisted index nodes, so use the Node file backend.
    store: { backend: ':file', path: tmpDir(), id: d.randomUuid() },
    writer: { backend: ':self', 'writer-ownership': ':exclusive' },
    'keep-history?': false
  };

  await d.createDatabase(config);
  const conn = await d.connect(config);
  await d.transact(conn, [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { name: 'before-branch' }
  ]);

  const originalDb = await d.db(conn);
  const originalCommit = await d.commitId(originalDb);
  if (typeof originalCommit !== 'string') {
    throw new Error('commitId should return a UUID string');
  }

  // Exercise the public GC boundary after an ordinary durable commit.
  const swept = await d.gcStorage(conn, new Date(), { 'min-age-ms': 0 });
  if (!Array.isArray(swept)) {
    throw new Error('gcStorage should return an array of reclaimed store keys');
  }

  await d.branch(conn, ':db', ':feature');
  const names = await d.branches(conn);
  if (!names.includes(':db') || !names.includes(':feature')) {
    throw new Error(`Expected :db and :feature branches, got ${names}`);
  }

  const featureDb = await d.branchAsDb(conn, ':feature');
  const originalByCommit = await d.commitAsDb(conn, d.uuid(originalCommit));
  if (!featureDb || !originalByCommit) {
    throw new Error('branchAsDb and commitAsDb should resolve snapshots');
  }

  await d.transact(conn, [{ name: 'main-after-branch' }]);
  const mergeReport = await d.mergeDb(
    conn,
    [':feature'],
    [{ name: 'merged-from-feature' }]
  );
  const parents = await d.parentCommitIds(mergeReport['db-after']);
  if (!Array.isArray(parents) || parents.length !== 2) {
    throw new Error(`Expected a two-parent merge commit, got ${parents}`);
  }

  // JavaScript arrays are converted to the parent set required by the core API.
  console.log('  Forcing and deleting snapshot branches...');
  await d.forceBranch(featureDb, ':snapshot', [':feature']);
  await d.deleteBranch(conn, ':feature');
  await d.deleteBranch(conn, ':snapshot');

  d.release(conn);
  await d.deleteDatabase(config);
  console.log('  ✓ Branch, commit, merge, force-branch, and GC APIs work from JavaScript');
}

async function testFilePersistence() {
  console.log('\n=== Test 8: File Backend Persistence ===');

  const dir = tmpDir();
  const config = {
    store: { backend: ':file', path: dir, id: d.randomUuid() }
  };
  
  console.log(`  Using temp directory: ${dir}`);
  
  console.log('  Creating database and adding data...');
  await d.createDatabase(config);
  let conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  const data = [{ name: 'Persisted Alice' }];
  await d.transact(conn, data);
  
  console.log('  ✓ Data transacted');
  
  console.log('  Releasing connection...');
  d.release(conn);
  
  console.log('  Reconnecting to same database...');
  conn = await d.connect(config);
  const db = await d.db(conn);
  
  console.log('  Checking persisted data...');
  const eid = await findEntityByName(db, 'Persisted Alice');
  if (!eid) throw new Error('Persisted data not found after reconnect');
  console.log('  ✓ Data persisted across reconnection');
  
  d.release(conn);
  
  console.log('  Deleting database...');
  await d.deleteDatabase(config);
  
  if (fs.existsSync(dir)) {
    throw new Error('Directory should be cleaned up');
  }
  console.log('  ✓ Database and directory cleaned up');
}

async function testSchemaRetrieval() {
  console.log('\n=== Test 9: Schema Retrieval ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  console.log('  Retrieving schema...');
  const db = await d.db(conn);
  const retrievedSchema = await d.schema(db);
  
  if (!retrievedSchema || Object.keys(retrievedSchema).length === 0) {
    throw new Error('Schema retrieval failed');
  }
  console.log(`  ✓ Schema retrieved: ${Object.keys(retrievedSchema).length} attributes`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testMultipleTransactions() {
  console.log('\n=== Test 10: Multiple Sequential Transactions ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':counter', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  console.log('  Transacting multiple values...');
  for (let i = 1; i <= 5; i++) {
    await d.transact(conn, [{ counter: i * 10 }]);
  }
  
  const db = await d.db(conn);
  const datoms = await d.datoms(db, ':eavt');
  const counterDatoms = datoms.filter(d => d.a && String(d.a) === ':counter');
  
  if (!counterDatoms || counterDatoms.length < 5) {
    throw new Error(`Expected at least 5 counter values, got ${counterDatoms ? counterDatoms.length : 0}`);
  }
  console.log(`  ✓ Multiple transactions completed: ${counterDatoms.length} counter datoms`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testQueryAPI() {
  console.log('\n=== Test 11: Query API (Datalog queries as EDN strings) ===');

  const config = {
    store: { backend: ':memory', id: d.randomUuid() }
  };
  
  await d.createDatabase(config);
  const conn = await d.connect(config);
  
  const schema = [
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':age', 'db/valueType': ':db.type/long', 'db/cardinality': ':db.cardinality/one' },
    { 'db/ident': ':email', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
  ];
  await d.transact(conn, schema);
  
  const data = [
    { name: 'Alice', age: 30, email: 'alice@example.com' },
    { name: 'Bob', age: 25, email: 'bob@example.com' },
    { name: 'Charlie', age: 35, email: 'charlie@example.com' }
  ];
  await d.transact(conn, data);
  
  const db = await d.db(conn);
  
  console.log('  Testing find-rel (return tuples)...');
  const q1 = await d.q('[:find ?e ?name :where [?e :name ?name]]', db);
  if (!q1 || q1.length !== 3) {
    throw new Error(`Expected 3 results, got ${q1 ? q1.length : 0}`);
  }
  console.log(`  ✓ Find-rel query returned ${q1.length} tuples`);
  
  console.log('  Testing find-coll (return single column)...');
  const q2 = await d.q('[:find [?name ...] :where [_ :name ?name]]', db);
  if (!q2 || q2.length !== 3) {
    throw new Error(`Expected 3 names, got ${q2 ? q2.length : 0}`);
  }
  console.log(`  ✓ Find-coll query returned ${q2.length} values`);
  
  console.log('  Testing find-tuple (return single tuple)...');
  const q3 = await d.q('[:find [?name ?age] :where [?e :name "Alice"] [?e :age ?age] [?e :name ?name]]', db);
  if (!q3 || q3.length !== 2) {
    throw new Error(`Expected tuple [name, age], got ${JSON.stringify(q3)}`);
  }
  if (q3[0] !== 'Alice' || q3[1] !== 30) {
    throw new Error(`Expected ["Alice", 30], got ${JSON.stringify(q3)}`);
  }
  console.log(`  ✓ Find-tuple query returned [${q3[0]}, ${q3[1]}]`);
  
  console.log('  Testing find-scalar (return single value)...');
  const q4 = await d.q('[:find ?name . :where [?e :name ?name] [?e :age 25]]', db);
  if (q4 !== 'Bob') {
    throw new Error(`Expected "Bob", got ${q4}`);
  }
  console.log(`  ✓ Find-scalar query returned: ${q4}`);
  
  console.log('  Testing query with multiple clauses...');
  const q5 = await d.q('[:find ?name ?age :where [?e :name ?name] [?e :age ?age] [(> ?age 28)]]', db);
  if (!q5 || q5.length !== 2) {
    throw new Error(`Expected 2 results (age > 28), got ${q5 ? q5.length : 0}`);
  }
  console.log(`  ✓ Multi-clause query returned ${q5.length} results`);
  
  console.log('  Testing query with input parameter...');
  const q6 = await d.q('[:find ?e :in $ ?name :where [?e :name ?name]]', db, 'Charlie');
  if (!q6 || q6.length !== 1) {
    throw new Error(`Expected 1 result for Charlie, got ${q6 ? q6.length : 0}`);
  }
  console.log(`  ✓ Parameterized query found entity for Charlie`);
  
  d.release(conn);
  await d.deleteDatabase(config);
}

async function testOptimisticOverlay() {
  console.log('\n=== Test 12: Optimistic Overlay ===');

  const config = { store: { backend: ':memory', id: d.randomUuid() } };
  await d.createDatabase(config);
  const conn = await d.connect(config);
  await d.transact(conn, [
    { 'db/ident': ':item/id', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one', 'db/unique': ':db.unique/identity' },
    { 'db/ident': ':name', 'db/valueType': ':db.type/string', 'db/cardinality': ':db.cardinality/one' }
  ]);

  const overlay = d.openOptimistic(conn);
  const events = [];
  const unsubscribe = d.optimisticListen(overlay, event => events.push(event));

  const handle = d.optimisticTransact(overlay, [{ 'item/id': 'writer', name: 'Immediate' }]);
  const visible = await d.q('[:find ?e . :where [?e :name "Immediate"]]', d.optimisticDb(overlay));
  if (visible == null) throw new Error('Optimistic value was not immediately visible');

  const committed = await handle.result;
  if (committed.status !== ':committed') {
    throw new Error(`Expected :committed, got ${committed.status}`);
  }
  if (events.length === 0 || !events.every(event => event['db-after'])) {
    throw new Error('Expected ordered snapshot transition events');
  }
  if (d.optimisticPending(overlay).length !== 0) {
    throw new Error('Committed overlay entry should reconcile with the base');
  }

  const invalid = d.optimisticTransact(overlay, [{ name: 42 }]);
  const rejected = await invalid.result;
  if (rejected.status !== ':rejected') {
    throw new Error(`Expected tagged :rejected result, got ${rejected.status}`);
  }

  const prediction = d.optimisticPredict(
    overlay,
    [{ 'item/id': 'prediction', name: 'Predicted' }],
    () => false,
    { 'timeout-ms': null }
  );
  const predicted = await d.q('[:find ?e . :where [?e :name "Predicted"]]', d.optimisticDb(overlay));
  if (predicted == null) throw new Error('Predicted value was not immediately visible');

  d.optimisticAck(overlay, prediction.ovId, { requestId: 'accepted' });
  const accepted = await prediction.result;
  if (accepted.status !== ':accepted' || accepted.receipt.requestId !== 'accepted') {
    throw new Error(`Expected accepted prediction, got ${JSON.stringify(accepted)}`);
  }
  d.optimisticReject(overlay, prediction.ovId, new Error('late failure'));
  if (d.optimisticPending(overlay).length !== 1) {
    throw new Error('A late rejection retracted an accepted prediction');
  }
  d.optimisticAbandon(overlay, prediction.ovId, ':test-complete');
  if (d.optimisticPending(overlay).length !== 0) {
    throw new Error('Abandoned prediction remained pending');
  }

  const failedPrediction = d.optimisticPredict(
    overlay,
    [{ 'item/id': 'rejected-prediction', name: 'Rejected Prediction' }],
    () => false
  );
  d.optimisticReject(overlay, failedPrediction.ovId, new Error('RPC rejected'));
  const failedResult = await failedPrediction.result;
  if (failedResult.status !== ':rejected') {
    throw new Error(`Expected rejected prediction, got ${failedResult.status}`);
  }

  unsubscribe();
  d.closeOptimistic(overlay);
  d.release(conn);
  await d.deleteDatabase(config);
  console.log('  ✓ Explicit overlay snapshots, predictions, and tagged results work');
}

// Main test runner
async function runAllTests() {
  console.log('╔════════════════════════════════════════════════════════╗');
  console.log('║  Comprehensive Datahike JavaScript API Test Suite     ║');
  console.log('╚════════════════════════════════════════════════════════╝');
  
  const startTime = Date.now();
  let passed = 0;
  let failed = 0;
  
  const tests = [
    testBasicOperations,
    testSchemaAndTransactions,
    testDatomsAPI,
    testPullAPI,
    testEntityAPI,
    testTemporalDatabases,
    testVersioningAndGc,
    testFilePersistence,
    testSchemaRetrieval,
    testMultipleTransactions,
    testQueryAPI,
    testOptimisticOverlay
  ];
  
  for (const test of tests) {
    try {
      await test();
      passed++;
    } catch (err) {
      failed++;
      console.error(`\n❌ Test failed: ${err.message}`);
      console.error(err.stack);
    }
  }
  
  const duration = Date.now() - startTime;
  
  console.log('\n╔════════════════════════════════════════════════════════╗');
  console.log('║                    Test Summary                        ║');
  console.log('╚════════════════════════════════════════════════════════╝');
  console.log(`  Total:    ${tests.length} tests`);
  console.log(`  Passed:   ${passed} ✓`);
  console.log(`  Failed:   ${failed} ✗`);
  console.log(`  Duration: ${duration}ms`);
  console.log('');
  
  if (failed === 0) {
    console.log('🎉 All tests passed!');
  } else {
    console.log(`⚠️  ${failed} test(s) failed`);
  }
  
  return failed === 0;
}

// Run tests and exit with timeout
runAllTests()
  .then(success => {
    // Force exit after 2 seconds to handle any lingering event loop activity
    setTimeout(() => {
      process.exit(success ? 0 : 1);
    }, 2000);
  })
  .catch(err => {
    console.error('Fatal error:', err);
    setTimeout(() => {
      process.exit(1);
    }, 2000);
  });
