# Datahike Norms - Database Migration System

The `datahike.norm.norm` namespace provides a database migration system for Datahike databases. Norms allow you to define schema changes and data migrations that are applied exactly once to your database.

## Overview

Norms are EDN files that define database migrations. Each norm contains transaction data (`tx-data`) and/or transaction functions (`tx-fn`) that are applied to the database. The norms system tracks which migrations have been applied using a special `:tx/norm` attribute, ensuring each migration runs only once.

## Core Functions

### `ensure-norms!`

```clojure
(ensure-norms! conn)
(ensure-norms! conn file-or-resource)
```

Takes a Datahike connection and optionally a java.io.File or java.net.URL to specify the location of your norms. Defaults to the resource `migrations`. All EDN files in the folder and its subfolders are considered migration files and will be transacted in lexicographical order by filename.

### `update-checksums!`

```clojure
(update-checksums!)
(update-checksums! norms-folder)
```

Computes checksums for all norm files and writes them to `checksums.edn`. This prevents inadvertent migrations when used with version control. Always run this after adding a new norm-file to your project.

### `verify-checksums`

```clojure
(verify-checksums file-or-resource)
```

Verifies that norm files haven't changed by comparing the files' checksums against stored checksums. Always run this before your migrations to ensure integrity.

## Norm File Format

Norm files are EDN files containing:
- `:tx-data` - Vector of transaction data to apply
- `:tx-fn` - Symbol referencing a transaction function that takes a connection and returns a vector of transactions

The filename (without extension) becomes the norm identifier keyword.

## File Organization

Files are processed in lexicographical order by name. Use numeric prefixes for ordering:

```
resources/migrations/
  001-initial-schema.edn
  002-add-users.edn
  003-data-migration.edn
  checksums.edn
```

## Examples

See test files in `test/datahike/norm/resources/` for working examples:
- `simple-test/` - Basic schema migrations
- `tx-fn-test/` - Using transaction functions
- `naming-and-sorting-test/` - File ordering examples

## Integration

The norms system automatically creates the `:tx/norm` attribute to track applied migrations. Each successfully applied norm is marked to prevent re-application.
