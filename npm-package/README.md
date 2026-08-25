# Datahike - JavaScript API

Durable Datalog database for JavaScript and Node.js, powered by ClojureScript.

## Features

- **Datalog Queries**: Expressive query language inspired by Datomic
- **Schema Support**: Optional schema with validation
- **Time Travel**: Access database history and temporal queries
- **Pluggable Backends**: Memory, file, or custom storage
- **Optional S3 Browser Backend**: Direct access to S3-compatible buckets
- **Promise-based API**: Native JavaScript async/await support
- **TypeScript Support**: Complete type definitions included

## Installation

```bash
npm install datahike
```

## Quick Start

```javascript
const d = require('datahike');

async function example() {
  // Create database configuration (requires UUID for :id)
  const config = {
    store: {
      backend: ':memory',
      id: d.randomUuid()
    }
  };

  // Create and connect to database
  await d.createDatabase(config);
  const conn = await d.connect(config);

  // Define schema
  // Keys: WITHOUT colon, Values: WITH colon
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
    }
  ];
  await d.transact(conn, schema);

  // Add data (data keys without colons)
  const data = [
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 }
  ];
  await d.transact(conn, data);

  // Query with Datalog
  const db = await d.db(conn);
  const results = await d.q(
    '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
    db
  );

  console.log(results); // [['Alice', 30], ['Bob', 25]]

  // Disconnect
  d.release(conn);
  await d.deleteDatabase(config);
}

example();
```

## Documentation

### S3-compatible storage in browsers

Import the opt-in build when a browser should persist Datahike directly to
Amazon S3, Cloudflare R2, MinIO, or another S3-compatible service:

```javascript
import * as d from 'datahike/s3';

const config = {
  store: {
    backend: ':s3',
    endpoint: 'https://s3.us-west-1.amazonaws.com',
    bucket: 'my-datahike-bucket',
    region: 'us-west-1',
    'access-key': temporaryCredentials.accessKeyId,
    secret: temporaryCredentials.secretAccessKey,
    'session-token': temporaryCredentials.sessionToken,
    id: d.randomUuid(),
    config: { 'optimistic-locking-retries': 10 }
  }
};

await d.createDatabase(config);
const conn = await d.connect(config);
```

The bucket must allow the browser origin through CORS and expose the `ETag`
response header. Use narrowly scoped, short-lived session credentials; never
ship long-lived bucket credentials in browser code. The regular `datahike`
entry does not include the S3 backend. Keep the store ID stable when reopening
a database. ETag locking must be enabled, as above, when more than one writer
can update the bucket.

### Configuration

**⚠️ Note:** Keyword syntax may change in future versions to simplify the API.

```javascript
const config = {
  store: {
    backend: ':memory',       // or ':file'
    id: d.randomUuid()        // Required: Datahike UUID identifier
  },
  // Optional configuration:
  'keep-history?': true,          // default: true
  'schema-flexibility': ':write'  // or ':read'
};

// File backend example (Node.js only)
const fileConfig = {
  store: {
    backend: ':file',
    path: './data'
  }
};
```

### Keywords

**Current keyword rules:**
- **Schema keys**: WITHOUT `:` prefix (`'db/ident'`, not `':db/ident'`)
- **Schema values**: WITH `:` prefix (`':name'`, `':db.type/string'`)
- **Data keys**: WITHOUT `:` prefix (`name`, `age`)
- **Pull patterns**: WITH `:` prefix (`[':name', ':age']`)

### Datalog Queries

Queries use EDN string format (Datalog DSL):

```javascript
// Find relationships
await d.q('[:find ?e ?name :where [?e :name ?name]]', db);

// Find collection
await d.q('[:find [?name ...] :where [_ :name ?name]]', db);

// With predicates
await d.q('[:find ?name :where [?e :name ?name] [?e :age ?age] [(> ?age 25)]]', db);

// Parameterized
await d.q('[:find ?e :in $ ?name :where [?e :name ?name]]', db, 'Alice');
```

### Pull API

Retrieve entity data by pattern:

```javascript
// Pull single entity
await d.pull(db, ['*'], entityId);

// Pull with specific attributes
await d.pull(db, [':name', ':age'], entityId);

// Pull many entities
await d.pullMany(db, ['*'], [id1, id2, id3]);
```

### Transactions

Add or retract data:

```javascript
// Entity maps (data keys without colons)
const data = [
  { name: 'Charlie', age: 35 }
];
await d.transact(conn, data);

// Tuple form
await d.transact(conn, [
  [':db/add', entityId, ':age', 36]
]);

// Retract
await d.transact(conn, [
  [':db/retract', entityId, ':age', 35]
]);
```

### Optimistic UI

Use an explicit overlay when a UI must show writes before the durable replica
catches up:

```javascript
const overlay = d.openOptimistic(conn);
const unsubscribe = d.optimisticListen(overlay, event => {
  render(event['db-after']);
});

const { result } = d.optimisticTransact(overlay, [
  [':db/add', entityId, ':age', 36]
]);
const outcome = await result; // { status: ':committed', ... } or ':rejected'

unsubscribe();
d.closeOptimistic(overlay);
```

Operation promises always resolve to tagged outcomes; a rejected transaction is
not a rejected JavaScript Promise. Externally owned RPCs can use
`optimisticPredict`, followed by `optimisticAck` or `optimisticReject`.

### Temporal Queries

Access database history:

```javascript
// Database at specific time
const historicalDb = d.asOf(d.db(conn), date);

// Full history
const historyDb = d.history(d.db(conn));
```

## API Reference

See [TypeScript definitions](index.d.ts) for complete API documentation.

### Core Functions

- `createDatabase(config)` - Create new database
- `deleteDatabase(config)` - Delete database
- `databaseExists(config)` - Check if database exists
- `connect(config)` - Connect to database
- `release(conn)` - Close connection
- `db(conn)` - Get current database value
- `transact(conn, txData)` - Execute transaction
- `q(query, ...args)` - Execute Datalog query
- `pull(db, pattern, entityId)` - Pull entity by pattern
- `pullMany(db, pattern, entityIds)` - Pull multiple entities
- `entity(db, entityId)` - Get entity (returns ClojureScript entity)
- `datoms(db, index, ...components)` - Access datoms directly
- `seekDatoms(db, index, ...components)` - Seek in index
- `schema(db)` - Get database schema
- `reverse_schema(db)` - Get reverse schema
- `metrics(db)` - Get database metrics

### Temporal Functions

- `asOf(db, timePoint)` - Database at specific time
- `since(db, timePoint)` - Changes since time
- `history(db)` - Full database history

## Known Limitations

- **Query API**: Requires EDN string format (no JavaScript object syntax)
- **Entity API**: Returns ClojureScript objects (use Pull API for plain JavaScript objects)
- **Keyword syntax**: May change in future versions for simplification
- **Advanced Datalog**: Some advanced features may have limited support

## License

Eclipse Public License 1.0

## Links

- [GitHub Repository](https://github.com/replikativ/datahike)
- [Documentation](https://github.com/replikativ/datahike/tree/master/doc)
- [ClojureScript API Docs](https://cljdoc.org/d/io.replikativ/datahike)
