# Datahike JavaScript API

**This is an experimental feature. Please try it out in a test environment and provide feedback.**

## Overview

The Datahike JavaScript API provides a Promise-based interface for Node.js and browser environments. All async operations return Promises, and data is automatically converted between JavaScript and ClojureScript.

## Project Structure

```
src/datahike/js/
  ├── api.cljs          # Main JS API implementation
  ├── api_macros.clj    # Macro for generating API functions
  ├── naming.cljc       # Shared naming conventions (ClojureScript → JavaScript)
  └── typescript.clj    # TypeScript definition generator

npm-package/
  ├── test.js                # Comprehensive test suite
  ├── package.template.json  # Version-controlled template
  ├── README.md             # npm package documentation
  ├── PUBLISHING.md         # Publishing guide
  └── index.d.ts            # Generated TypeScript definitions

bb/src/tools/
  └── npm.clj              # Build automation (version, types, compile, test)
```

## Build Configuration

The JS API uses a modern automated build pipeline:

```bash
# Full build: version + types + compile + test
bb npm-build

# Individual steps:
bb npm-version    # Generate package.json from template with version from config.edn
bb npm-types      # Generate TypeScript definitions
bb npm-test       # Run npm package tests
```

Versions are automatically calculated as `major.minor.commit-count` from `config.edn`.

Output is generated in `npm-package/datahike.js.api.js` with advanced compilation.

## Installation

```bash
npm install datahike
```

## Usage

### Basic Example

```javascript
const d = require('datahike');

async function example() {
  // Configuration
  const config = {
    store: { backend: ':mem', id: 'my-db' }
  };
  
  // Create database
  await d.createDatabase(config);
  
  // Connect
  const conn = await d.connect(config);
  
  // Define schema (note: keywords need ':' prefix)
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
  
  // Insert data
  const data = [
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 }
  ];
  await d.transact(conn, data);
  
  // Get database value
  const db = d.db(conn);
  
  // Get datoms
  const datoms = await d.datoms(db, ':eavt');
  console.log('Datoms:', datoms.length);
  
  // Pull API
  const entityId = 1; // Find ID through query or datoms
  const pulled = await d.pull(db, ['name', 'age'], entityId);
  console.log('Entity:', pulled);
  
  // Clean up
  d.release(conn);
  await d.deleteDatabase(config);
}
```

## Data Conversion

### JavaScript → ClojureScript

- JS objects → CLJ maps with keyword keys
- Arrays → CLJ vectors
- Strings starting with `:` → keywords (for values only)
- Other values pass through unchanged

### ClojureScript → JavaScript

- Keywords → strings with `:` prefix
- CLJ maps → JS objects (keyword keys become strings without `:`)
- Vectors/Lists → Arrays
- Sets → Arrays
- **Special**: DB values, datoms, and connections pass through unchanged

## Important Notes

### Keyword Syntax

Keywords **must** include the `:` prefix in strings:

```javascript
// ✅ Correct
const schema = [{
  'db/ident': ':name',           // Good
  'db/valueType': ':db.type/string'  // Good
}];

// ❌ Wrong
const schema = [{
  'db/ident': 'name',            // Bad - missing ':'
  'db/valueType': 'db.type/string'   // Bad - missing ':'
}];
```

### Backend Configuration

```javascript
// In-memory backend
const memConfig = {
  store: { backend: ':mem', id: 'my-db' }
};

// File backend (Node.js only)
const fileConfig = {
  store: { backend: ':file', path: '/path/to/db' }
};
```

### Async Operations

All database operations return Promises:

```javascript
// Use await
All functions are exported with camelCase naming. The JavaScript API automatically:
- Converts `kebab-case` → `camelCase`
- Removes `!` and `?` suffixes
- Renames `with` → `withDb` (reserved keyword)
- Filters out incompatible functions (e.g., synchronous `transact`)

**Main API Functions:**
- **Database Lifecycle**: `createDatabase`, `deleteDatabase`, `databaseExists`
- **Connection**: `connect`, `release`
- **Database Values**: `db`, `asOf`, `since`, `history`, `withDb`
- **Transactions**: `transact` (async, returns Promise)
- **Queries**: `q`, `pull`, `pullMany`, `datoms`, `seekDatoms`, `entity`, `entityDb`
- **Schema**: `schema`, `reverseSchema`
- **Utilities**: `tempid`, `isFiltered`, `filter`, `indexRange`
- **Maintenance**: `gcStorage`
- **Info**: `datahikeVersion`

Note: `transact!` from Clojure becomes `transact` in JavaScript (the `!` is removed).
```javascript
const result = await d.transact(conn, data);
console.log(result['tx-data']);      // Note: 'tx-data' not 'tx_data'
console.log(result['db-before']);
console.log(result['db-after']);
```

## API Functions

The following functions are exported (camelCase naming):

- **Database Lifecycle**: `createDatabase`, `deleteDatabase`, `databaseExists`
- **Connection**: `connect`, `release`
- **Database Values**: `db`, `asOf`, `since`, `history`, `withDb`, `dbWith`
- **Transactions**: `transact` (async), `loadEntities`
- **Queries**: `q`, `pull`, `pullMany`, `datoms`, `seekDatoms`, `entity`, `entityDb`
- **ypeScript Support

Full TypeScript definitions are automatically generated and included:

```typescript
import * as d from 'datahike';

interface Config {
  store: {
    backend: string;
    id?: string;
    path?: string;
  };
  'keep-history'?: boolean;
  'schema-flexibility'?: string;
}

const config: Config = {
  store: { backend: ':mem', id: 'example' }
};

async function example() {
  await d.createDatabase(config);
  const conn = await d.connect(config);
  const db = d.db(conn);
  // TypeScript will check types automatically
}
```

## Testing

Run Naming Conventions

Naming is centralized in `src/datahike/js/naming.cljc` for consistency:

```clojure
;; Functions to skip (incompatible with JS or aliases)
(def js-skip-list #{'transact})  ; sync version, use transact! instead

;; Conversion rules:
;; database-exists? → databaseExists
;; create-database  → createDatabase
;; transact!        → transact (! removed)
;; with             → withDb (reserved keyword)
```

### Adding New Functions

1. **Add to API specification**: Function must be in `datahike.api.specification`
2. **Update skip list** (if needed): Add to `js-skip-list` in `naming.cljc` to exclude
3. **Rebuild package**:
   ```bash
   bb npm-build
   ```
   This will:
   - Generate TypeScript definitions (using naming.cljc)
   - Compile API functions (using api_macros.clj)
   - Run tests

### Version Management

Versions are calculated automatically from `config.edn`:
- Format: `major.minor.commit-count`
- Example: `0.6.1637` (major: 0, minor: 6, commits: 1637)
- `package.json` is generated from `package.template.json` during build

### Publishing to npm

See `npm-package/PUBLISHING.md` for detailed publishing instructions.

Quick workflow:
```bash
# 1. Build and test
bb npm-build

# 2. Verify package
cd npm-package && npm pack --dry-run

# 3. Publish
npm publish
``

The test suite includes:
- Basic database operations
- Schema and transactions
- Datoms API
- Pull API
- Entity API
- Temporal databases (history, as-of)
- File backend persistence
- Schema retrieval
- Query API

## CI/CD Integration

The npm tests are integrated into the main CI/CD pipeline:

```bash
bb check  # Runs: test, npm-test, format, lint, outdated
```

## Known Limitations

### Query API

The Datalog query API works with EDN string format:

```javascript
// ✅ Works: EDN string format
const results = await d.q(
  '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
  db
);

// ✅ Also works: Datoms API (recommended for simple queries)
const db = d.db(conn);
const datoms = await d.datoms(db, ':eavt');
```

### Compilation Warnings

Shadow-cljs compilation may produce warnings from dependencies:
- **BigInt warnings** from `persistent-sorted-set` (harmless, ES2020 feature)
- **Infer warnings** from dependencies (cosmetic, doesn't affect functionality)
- **Redef warning** for `filter` (expected, intentional override)
// const results = await d.q({ find: '?e', where: [...] }, db);
```

### BigInt Warnings

Compilation produces harmless warnings about ES2020 BigInt usage from `persistent-sorted-set`. These can be ignored - the code works correctly.

## Development

### Adding New Functions

Functions are automatically generated from `datahike.api.specification` by the `emit-js-api` macro. To add a function:

1. Add it to `api-specification` in `src/datahike/api/specification.cljc`
2. Add to `js-skip-list` in `api_macros.clj` if it should be excluded
3. Rebuild with `npx shadow-cljs compile npm-release`

### Data Conversion Rules

If you need to handle special types, update `clj->js-recursive` and `js->clj-recursive` in `src/datahike/js/api.cljs`.

## Next Steps

### API Improvements to Consider
- [ ] Simplify keyword syntax (auto-detect vs requiring `:` prefix)
- [ ] Add transaction builder helpers for common patterns
- [ ] Consider fluent/chainable API for queries
- [ ] Add convenience wrappers for common query patterns
- [ ] Improve error messages for JavaScript users

### Publishing
- [ ] Publish to npm

## See Also

- [Datahike Documentation](../README.md)
- [Configuration Guide](config.md)
- [Schema Documentation](schema.md)
