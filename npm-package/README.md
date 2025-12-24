# Datahike - JavaScript API

Durable Datalog database for JavaScript and Node.js, powered by ClojureScript.

## Features

- **Datalog Queries**: Expressive query language inspired by Datomic
- **Schema Support**: Optional schema with validation
- **Time Travel**: Access database history and temporal queries
- **Pluggable Backends**: Memory, file, or custom storage
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
  // Create database configuration
  const config = {
    store: { backend: ':mem', id: 'example' }
  };

  // Create and connect to database
  await d.createDatabase(config);
  const conn = await d.connect(config);

  // Define schema
  await d.transact(conn, {
    'tx-data': [
      {
        ':db/ident': ':name',
        ':db/valueType': ':db.type/string',
        ':db/cardinality': ':db.cardinality/one'
      },
      {
        ':db/ident': ':age',
        ':db/valueType': ':db.type/long',
        ':db/cardinality': ':db.cardinality/one'
      }
    ]
  });

  // Add data
  await d.transact(conn, {
    'tx-data': [
      { ':name': 'Alice', ':age': 30 },
      { ':name': 'Bob', ':age': 25 }
    ]
  });

  // Query with Datalog
  const db = d.db(conn);
  const results = await d.q(
    '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
    db
  );

  console.log(results); // [['Alice', 30], ['Bob', 25]]

  // Disconnect
  await d.releaseConnection(conn);
}

example();
```

## Documentation

### Configuration

Datahike requires configuration for database backend. All keys use keyword syntax (`:key`):

```javascript
const config = {
  store: {
    backend: ':mem',  // or ':file', ':level', etc.
    id: 'my-db',      // optional: database identifier
    path: './data'    // for :file backend
  },
  'keep-history': true,  // default: true
  'schema-flexibility': ':write'  // or ':read'
};
```

### Keywords

Datahike uses keywords (`:keyword`) extensively. In JavaScript:
- Prefix with colon: `:name`, `:db/ident`
- Use in queries, schema, and data

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
// Entity maps
await d.transact(conn, {
  'tx-data': [
    { ':name': 'Charlie', ':age': 35 }
  ]
});

// Tuple form
await d.transact(conn, {
  'tx-data': [
    [':db/add', entityId, ':age', 36]
  ]
});

// Retract
await d.transact(conn, {
  'tx-data': [
    [':db/retract', entityId, ':age', 35]
  ]
});
```

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
- `releaseConnection(conn)` - Close connection
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

- Entity API returns ClojureScript objects (use Pull API for plain data)
- Keywords must be prefixed with `:` in JavaScript
- Some advanced Datalog features may have limited support

## License

Eclipse Public License 1.0

## Links

- [GitHub Repository](https://github.com/replikativ/datahike)
- [Documentation](https://github.com/replikativ/datahike/tree/master/doc)
- [ClojureScript API Docs](https://cljdoc.org/d/io.replikativ/datahike)
