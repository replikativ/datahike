// Compile-only contract test for the public Datahike JavaScript API.

import * as d from './index';

async function typescriptTest() {
  const configuredLogLevel: d.LogLevel = d.setLogLevel('warn');

  // DatabaseConfig type is now properly typed
  const config: d.DatabaseConfig = {
    store: {
      backend: ':memory',
      id: d.randomUuid()
    },
    'keep-history?': true,
    'schema-flexibility': ':write'
  };

  const s3Store: d.S3StoreConfig = {
    backend: ':s3',
    id: d.uuid('550e8400-e29b-41d4-a716-446655440000'),
    endpoint: 'https://s3.us-west-1.amazonaws.com',
    bucket: 'datahike-test',
    'access-key': 'temporary-access-key',
    secret: 'temporary-secret',
    region: 'us-west-1',
    'path-style?': false,
    config: { 'optimistic-locking-retries': 10 }
  };
  const s3Config: d.DatabaseConfig = { store: s3Store };

  // UUID strings returned by the API are deliberately distinct from UUID
  // inputs, which must be constructed at the JavaScript boundary.
  const uuidOutput: d.UuidValue = '550e8400-e29b-41d4-a716-446655440000';
  const invalidConfig: d.DatabaseConfig = {
    // @ts-expect-error Raw UUID strings are output values, not valid store IDs.
    store: { backend: ':memory', id: uuidOutput }
  };

  // Check if database exists - returns boolean
  const exists: boolean = await d.databaseExists(config);

  // Create database
  await d.createDatabase(config);
  
  // Connect returns Connection type
  const conn: d.Connection = await d.connect(config);

  // Transaction with typed Transaction array
  const transactions: d.Transaction[] = [
    { ':db/ident': ':name', ':db/valueType': ':db.type/string', ':db/cardinality': ':db.cardinality/one' },
    { ':name': 'Alice', ':age': 30 }
  ];

  // TransactionReport is properly typed
  const txResult: d.TransactionReport = await d.transact(conn, transactions);
  console.log('Temp IDs:', txResult.tempids);
  console.log('TX data:', txResult['tx-data']); // Datom[]

  // Get database - returns Database type
  const db: d.Database = await d.db(conn);

  // Query with typed arguments (query as string or array, optional limit/offset)
  const queryResults = await d.q<Array<[string, number]>>({
    query: '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
    args: [db],
    limit: 10
  });

  // Pull API with typed options
  const pullResult = await d.pull<{ name: string; age: number }>(db, {
    selector: [':name', ':age'],
    eid: 1  // number | string
  });

  // Schema returns Schema type
  const schema: d.Schema = await d.schema(db);

  const historyDb: d.Database = await d.history(db);

  const temporaryId: number = await d.tempid(':db.part/user');

  // Temporal query - asOf returns Database
  const pastDb: d.Database = await d.asOf(db, Date.now());

  // Versioning and GC use ordinary JavaScript arrays/objects at the boundary.
  const branchNames: d.BranchName[] = await d.branches(conn);
  await d.branch(conn, ':db', ':feature');
  const commitOutput: d.UuidValue | null = await d.commitId(db);
  if (commitOutput) {
    const committedDb: d.Database | null = await d.commitAsDb(conn, d.uuid(commitOutput));
    console.log(committedDb);
  }
  const branchDb: d.Database | null = await d.branchAsDb(conn, ':feature');
  const parentIds: d.UuidValue[] | null = await d.parentCommitIds(db);
  if (branchDb) {
    await d.forceBranch(branchDb, ':snapshot', [':feature']);
  }
  const mergeReport: d.TransactionReport = await d.mergeDb(
    conn,
    [':feature'],
    [{ ':name': 'merged' }]
  );
  const reclaimed: unknown[] = await d.gcStorage(conn, new Date(0), {
    'min-age-ms': 60_000
  });

  // Optimistic overlays are explicit resources. Reads and subscriptions are
  // synchronous; each submitted operation exposes a tagged-result Promise.
  const overlay: d.OptimisticOverlay = d.openOptimistic(conn, {
    'prediction-timeout-ms': 30_000
  });
  const unsubscribe = d.optimisticListen(overlay, event => {
    const snapshot: d.Database = event['db-after'];
    console.log('Overlay revision:', event.revision, snapshot);
  });
  const optimistic: d.OptimisticHandle = d.optimisticTransact(
    overlay,
    [{ ':name': 'Optimistic Alice' }]
  );
  const optimisticResult: d.OptimisticResult = await optimistic.result;
  console.log('Optimistic status:', optimisticResult.status);
  unsubscribe();
  d.closeOptimistic(overlay);

  console.log(queryResults, pullResult, schema, historyDb, pastDb, s3Config,
              uuidOutput, invalidConfig, temporaryId, branchNames, branchDb,
              parentIds, mergeReport, reclaimed, configuredLogLevel);
}

void typescriptTest;
