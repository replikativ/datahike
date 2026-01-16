# Datahike Documentation

Welcome to the Datahike documentation! This index organizes all documentation by topic and user level.

## 🚀 Getting Started

New to Datahike? Start here:

- **[Main README](../README.md)** - Project overview, installation, and quick start
- **[Why Datalog?](./datalog-vs-sql.md)** - Query comparisons and when to use Datalog
- **[Configuration](./config.md)** - Database configuration options
- **[Schema Flexibility](./schema.md)** - Schema-on-read vs schema-on-write

## 📚 Core Concepts

Essential concepts for working with Datahike:

- **[Time Variance](./time_variance.md)** - Time-travel queries (as-of, history, since), audit trails, and GDPR-compliant purging
- **[Storage Backends](./storage-backends.md)** - Choosing the right backend for your needs (file, memory, PostgreSQL, etc.)
- **[Garbage Collection](./gc.md)** - Reclaim storage by removing old database snapshots
- **[Entity Spec](./entity_spec.md)** - Entity API and specifications
- **[Logging and Error Handling](./logging_and_error_handling.md)** - Debugging and error management

## 🔧 Language Bindings (Beta)

Datahike supports multiple languages and platforms:

### JVM Languages
- **[Java API](./java-api.md)** - Fluent builder API, automatic collection conversion, comprehensive bindings
  - Examples: [`examples/java/`](../examples/java/)
  - Status: **Beta** - Functional and tested, but may receive breaking changes

### JavaScript/TypeScript
- **[JavaScript API](./javascript-api.md)** - Promise-based API for Node.js and browsers
  - npm: `npm install datahike@next`
  - Status: **Beta** - Functional and tested, but may receive breaking changes

### ClojureScript
- **[ClojureScript Support](./cljs-support.md)** - Async operations, Node.js, IndexedDB, and browser backends
  - Backends: Memory, IndexedDB, TieredStore, File (Node.js)
  - Status: **Beta** - Functional, please try it out and provide feedback

### Python
- **[Python Bindings](../pydatahike/README.md)** - High-level Pythonic API with automatic EDN conversion
  - Status: **Beta** - Functional and tested, but may receive breaking changes

### CLI & Shell
- **[CLI (dthk)](./cli.md)** - Native command-line tool compiled with GraalVM
  - Features: Instant startup, file backend, scriptable
  - Status: **Beta** - Functional and tested, but command structure may change

- **[Babashka Pod](./bb-pod.md)** - Shell scripting with Datahike
  - Integration: Native pod for Babashka
  - Status: **Beta** - Functional but not yet used in production

### Native Libraries
- **[libdatahike](./libdatahike.md)** - C/C++ native bindings for embedding in non-JVM applications
  - Use cases: Embedding in C/C++/Rust/Go applications
  - Status: **Beta** - Functional and tested, but API may change

- **[EDN Conversion Rules](./bindings/edn-conversion.md)** - How data types map between languages

## 🏗️ Advanced Features

For experienced users building production systems:

- **[Distributed Architecture](./distributed.md)** - Distributed Index Space and real-time sync with Kabel
- **[Versioning](./versioning.md)** - Git-like branching and merging (beta)
- **[Norms](./norms.md)** - Database migration system
- **[Unstructured Input Support](./unstructured.md)** - Schema inference from JSON/EDN (experimental)

## 🔍 Reference

- **[Differences to Datomic](./datomic_differences.md)** - API compatibility and differences
- **[Benchmarking](./benchmarking.md)** - Performance testing and optimization

## 🛠️ Development

Contributing to Datahike:

- **[Contributing Guide](./contributing.md)** - How to contribute to the project
- **[Backend Development](./backend-development.md)** - Creating custom storage backends

---

## Feature Maturity Levels

- **Stable** - Production-ready, API unlikely to change
- **Beta** - Tested and functional, but API may receive changes
- **Experimental** - Try it out, API likely to change significantly

## Need Help?

- 💬 **[Discussions](https://github.com/replikativ/datahike/discussions)** - Ask questions and share ideas
- 🐛 **[Issues](https://github.com/replikativ/datahike/issues)** - Report bugs or request features
- 📖 **[API Docs](https://cljdoc.org/d/org.replikativ/datahike)** - Complete API reference on cljdoc
