// TypeScript type checking test for Datahike
// This file demonstrates TypeScript IDE support and improved types

import * as d from './index';

async function typescriptTest() {
  // DatabaseConfig type is now properly typed
  const config: d.DatabaseConfig = {
    store: {
      backend: ':mem',
      id: 'ts-test'
    },
    'keep-history': true,
    'schema-flexibility': 'write'
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
  const db: d.Database = d.db(conn);

  // Query with typed arguments (query as string or array, optional limit/offset)
  const queryResults = await d.q({
    query: '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
    args: [db],
    limit: 10
  });

  // Pull API with typed options
  const pullResult = await d.pull(db, {
    selector: [':name', ':age'],
    eid: 1  // number | string
  });

  // Schema returns Schema type
  const schema: d.Schema = await d.schema(db);

  // History returns Array<any>
  const historyDb: Array<any> = await d.history(db);

  // Temporal query - asOf returns Database
  const pastDb: d.Database = await d.asOf(db, Date.now());

  // Version is string (not Promise)
  const version: string = d.version();

  console.log('TypeScript types work perfectly!');
  console.log('All parameters and return types are properly typed');
}

// This file demonstrates type checking - uncomment to test IDE support:
// typescriptTest();
