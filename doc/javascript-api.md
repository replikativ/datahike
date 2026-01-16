# Datahike JavaScript API

**Status: Beta** - API is functional and tested, but may receive breaking changes. Published as `datahike@next` on npm.

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
bb codegen-ts     # Generate TypeScript definitions
bb npm-test       # Run npm package tests
```

Versions are automatically calculated as `major.minor.commit-count` from `config.edn`.

Output is generated in `npm-package/datahike.js.api.js` with advanced compilation.

## Installation

```bash
npm install datahike@next
```

## Usage

### Basic Example

```javascript
const d = require('datahike');
const crypto = require('crypto');

async function example() {
  // Configuration - must use UUID for :id
  const config = {
    store: {
      backend: ':memory',
      id: crypto.randomUUID()
    }
  };

  // Create database
  await d.createDatabase(config);

  // Connect
  const conn = await d.connect(config);

  // Define schema
  // Keys: WITHOUT colon (plain strings)
  // Values: WITH colon prefix (keywords)
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

  // Insert data (data keys without colons)
  const data = [
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 }
  ];
  await d.transact(conn, data);

  // Get database value (synchronous)
  const db = await d.db(conn);

  // Get datoms
  const datoms = await d.datoms(db, ':eavt');
  console.log('Datoms:', datoms.length);

  // Pull API (pattern attributes with colons)
  const entityId = 1; // Find ID through query or datoms
  const pulled = await d.pull(db, [':name', ':age'], entityId);
  console.log('Entity:', pulled);

  // Clean up
  d.release(conn);
  await d.deleteDatabase(config);
}
```

## Data Conversion

Datahike uses universal EDN conversion rules that are consistent across Python, JavaScript, and Java bindings:

> **Keys are always keywordized. Values starting with `:` become keywords, everything else remains literal.**

### JavaScript → ClojureScript

- **Object keys**: Always converted to keywords (`:` prefix added automatically)
- **String values starting with `:`**: Converted to keywords (e.g., `":memory"` → `:memory`)
- **String values starting with `\\:`**: Literal colon string (e.g., `"\\:literal"` → `":literal"`)
- **Other string values**: Remain as strings
- **Arrays**: Converted to CLJ vectors
- **Numbers, booleans, null**: Pass through unchanged
- **UUID strings**: Auto-detected and converted to UUID objects (convenience feature)

### ClojureScript → JavaScript

- **Keywords**: Converted to strings with `:` prefix
- **CLJ maps**: Converted to JS objects (keyword keys become strings)
- **Vectors/Lists**: Converted to Arrays
- **Sets**: Converted to Arrays
- **Special**: DB values, datoms, and connections pass through unchanged

For complete conversion rules and edge cases, see [EDN Conversion Documentation](bindings/edn-conversion.md).

## Important Notes

### Keyword Syntax

The universal EDN conversion rules make keyword syntax simple and predictable:

**Simple rule: Keys never need `:`, values that should be keywords need `:`**

```javascript
// ✅ Schema definition
const schema = [{
  'db/ident': ':name',              // Key auto-keywordized, value is keyword
  'db/valueType': ':db.type/string', // Both become keywords
  'db/cardinality': ':db.cardinality/one'
}];

// ✅ Data insertion
const data = [
  { name: 'Alice', age: 30 }        // Keys auto-keywordized, values are literals
];

// ✅ Configuration
const config = {
  store: {
    backend: ':memory',                // ":memory" becomes :memory keyword
    id: 'test'                      // "test" stays as string
  },
  'schema-flexibility': ':read',    // ":read" becomes :read keyword
  'keep-history?': true             // boolean passes through
};

// ✅ Pull patterns (array of keyword strings)
const pattern = [':name', ':age'];   // Strings with : become keywords

// ✅ Literal colon strings (rare)
const data = [{
  description: '\\:starts-with-colon'  // Escaped → ":starts-with-colon" (string)
}];
```

### Backend Configuration

```javascript
const crypto = require('crypto');

// In-memory backend (requires UUID)
const memConfig = {
  store: {
    backend: ':memory',
    id: crypto.randomUUID()
  }
};

// File backend (Node.js only)
const fileConfig = {
  store: {
    backend: ':file',
    path: '/path/to/db'
  }
};
```

### Async Operations

All database operations return Promises. Use `await` or `.then()`:

```javascript
// Using await (recommended)
const conn = await d.connect(config);
const result = await d.transact(conn, data);
console.log(result['tx-data']);      // Note: 'tx-data' not 'tx_data'
console.log(result['db-before']);
console.log(result['db-after']);

// Using promises
d.connect(config).then(conn => {
  return d.transact(conn, data);
}).then(result => {
  console.log(result['tx-data']);
});
```

## API Functions

All functions use camelCase naming. The JavaScript API automatically:
- Converts `kebab-case` → `camelCase`
- Removes `!` and `?` suffixes
- Renames `with` → `withDb` (reserved keyword)

**Main API Functions:**
- **Database Lifecycle**: `createDatabase`, `deleteDatabase`, `databaseExists`
- **Connection**: `connect`, `release`
- **Database Values**: `db`, `asOf`, `since`, `history`, `withDb`, `dbWith`
- **Transactions**: `transact` (async, returns Promise), `loadEntities`
- **Queries**: `q`, `pull`, `pullMany`, `datoms`, `seekDatoms`, `entity`, `entityDb`
- **Schema**: `schema`, `reverseSchema`
- **Utilities**: `tempid`, `isFiltered`, `filter`, `indexRange`
- **Maintenance**: `gcStorage`
- **Info**: `datahikeVersion`

Note: `transact!` from Clojure becomes `transact` in JavaScript (the `!` is removed).

## TypeScript Support

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
  store: { backend: ':memory', id: 'example' }
};

async function example() {
  await d.createDatabase(config);
  const conn = await d.connect(config);
  const db = d.db(conn);
  // TypeScript will check types automatically
}
```

## Testing

The comprehensive test suite in `npm-package/test.js` covers all functionality:
- Basic database operations
- Schema and transactions
- Datoms API
- Pull API
- Entity API
- Temporal databases (history, as-of, since)
- File backend persistence
- Query API

Run tests with:
```bash
bb npm-test
```

Tests are automatically run in CI/CD as part of `bb check`.

## Naming Conventions

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

# 3. Publish as next
npm publish --tag next
```

## Known Limitations

### Query API

The Datalog query API requires EDN string format:

```javascript
// ✅ Works: EDN string format
const results = await d.q(
  '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
  db
);

// ❌ Doesn't work: JavaScript object syntax
// const results = await d.q({ find: '?e', where: [...] }, db);

// ✅ Alternative: Use Datoms API for simple queries
const datoms = await d.datoms(db, ':eavt');
```

### Entity API

The `entity` function returns ClojureScript objects, not plain JavaScript objects. Use the Pull API for plain data:

```javascript
// ✅ Recommended: Pull API returns plain objects
const data = await d.pull(db, [':name', ':age'], entityId);
console.log(data.name); // Works

// ⚠️ Entity API returns ClojureScript objects
const entity = await d.entity(db, entityId);
// Accessing attributes requires understanding ClojureScript objects
```

### Compilation Warnings

Shadow-cljs compilation may produce warnings from dependencies:
- **BigInt warnings** from `persistent-sorted-set` (harmless, ES2020 feature)
- **Infer warnings** (cosmetic, doesn't affect functionality)
- **Redef warning** for `filter` (expected, intentional override)

These can be safely ignored.

## Development

### Adding New Functions

Functions are automatically generated from `datahike.api.specification` by the `emit-js-api` macro. To add a function:

1. Add it to `api-specification` in `src/datahike/api/specification.cljc`
2. Add to `js-skip-list` in `api_macros.clj` if it should be excluded
3. Rebuild with `npx shadow-cljs compile npm-release`

### Data Conversion Rules

If you need to handle special types, update `clj->js-recursive` and `js->clj-recursive` in `src/datahike/js/api.cljs`.

## Current Release

Published as `datahike@next` on npm. Install with:
```bash
npm install datahike@next
```

## Future Improvements

- Add transaction builder helpers for common patterns
- Fluent/chainable API for queries
- Convenience wrappers for common query patterns
- Improved error messages for JavaScript users
- Helper module with EDN type constructors (similar to Python's `edn.keyword()`, `kw.DB_ID` constants)

## See Also

- [Datahike Documentation](../README.md)
- [Configuration Guide](config.md)
- [Schema Documentation](schema.md)
