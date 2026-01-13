# Backend Development

Datahike storage is built on top of [konserve](https://github.com/replikativ/konserve), a universal key-value store abstraction. To add a new storage backend to Datahike, you simply implement a konserve backend. Once your konserve backend is required and registered, Datahike will transparently work on top of it.

## Implementation Steps

1. **Implement a konserve backend** following the [konserve documentation](https://github.com/replikativ/konserve)
2. **Register your backend** by requiring it in your project
3. **Use it with Datahike** by specifying the backend keyword in your configuration

That's it! Datahike will automatically work with any konserve backend.

## Konserve Backend Documentation

See the [konserve documentation](https://github.com/replikativ/konserve) for:
- How to implement a new backend
- The konserve protocol specification
- Examples of existing backends

## Optional: Additional Datahike-Specific Support

While konserve backends work out of the box with Datahike, you can optionally add Datahike-specific optimizations:

### Serialization Handlers

For performance optimization, you can add custom serialization handlers for Datahike's data structures. See [datahike-lmdb](https://github.com/replikativ/datahike-lmdb) as an example, which installs LMDB-specific Datahike serialization handlers.

## Existing Backends

Datahike ships with built-in support for:
- `:memory` - In-memory storage (via konserve)
- `:file` - File-based storage (via konserve)

Additional backends available through konserve:
- [datahike-lmdb](https://github.com/replikativ/datahike-lmdb) - LMDB backend with optimized serialization
- [IndexedDB](https://github.com/replikativ/konserve-indexeddb) - Browser storage (via konserve)
- [PostgreSQL](https://github.com/replikativ/konserve-pg) - PostgreSQL backend (via konserve)
- Many more available in the [konserve ecosystem](https://github.com/replikativ/konserve)

## Configuration

Once your konserve backend is available, use it with Datahike by specifying the backend keyword:

```clojure
(require '[datahike.api :as d])

;; Example: using a custom backend
(def cfg {:store {:backend :your-backend-keyword
                  :id #uuid "550e8400-e29b-41d4-a716-446655440000"
                  ;; ... backend-specific configuration
                  }})

(d/create-database cfg)
(def conn (d/connect cfg))
```

## Advanced: Custom Index Implementations

While less common than implementing backends, you can also implement custom index data structures for Datahike. This allows exploring different index algorithms optimized for specific use cases.

Datahike uses **persistent-set** as the default index. The **hitchhiker-tree** index (`:datahike.index/hitchhiker-tree`) is available for backwards compatibility and can be useful for certain workloads.

To implement a custom index:

1. Implement the index protocol (see existing implementations in `src/datahike/index/`)
2. Register your index implementation
3. Use it by specifying `:index :your-index-keyword` in the database configuration

Example configuration with custom index:

```clojure
(def cfg {:store {:backend :memory
                  :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
          :index :datahike.index/hitchhiker-tree  ; or your custom index
          :schema-flexibility :write})
```

This is an advanced topic primarily useful for research or highly specialized performance requirements.
