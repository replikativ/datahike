<p align="center">
  <a align="center" href="https://datahike.io" target="_blank">
    <img alt="Datahike" src="./doc/assets/datahike-logo.svg" height="128em">
  </a>
</p>
<p align="center">
<a href="https://clojurians.slack.com/archives/CB7GJAN0L"><img src="https://badgen.net/badge/-/slack?icon=slack&label"/></a>
<a href="https://clojars.org/org.replikativ/datahike"> <img src="https://img.shields.io/clojars/v/org.replikativ/datahike.svg" /></a>
<a href="https://circleci.com/gh/replikativ/datahike"><img src="https://circleci.com/gh/replikativ/datahike.svg?style=shield"/></a>
<a href="https://github.com/replikativ/datahike/tree/main"><img src="https://img.shields.io/github/last-commit/replikativ/datahike/main"/></a>
</p>

**Branch databases, not just code.**

[Datahike](https://datahike.io) is a durable [Datalog](https://en.wikipedia.org/wiki/Datalog) database with
Datomic-compatible APIs and git-like semantics. Built on persistent data structures and structural sharing,
database snapshots are immutable values that can be held, shared, and queried anywhere—without locks or copying.

**Key capabilities:**
- 🌐 **[Distributed Index Space](./doc/distributed.md)**: Read scaling without database connections—readers access persistent indices directly
- 🗄️ **[Flexible storage](./doc/storage-backends.md)**: File, LMDB, S3, JDBC, Redis, IndexedDB via konserve
- 🌍 **[Cross-platform](./doc/README.md#language-bindings-beta)**: JVM, Node.js, Browser (Clojure, ClojureScript, JavaScript, Java APIs)
- ⚡ **[Real-time sync](./doc/distributed.md)**: WebSocket streaming with Kabel for browser ↔ server
- 🕰️ **[Time-travel](./doc/time_variance.md)**: Query any historical state, full transaction audit trail
- 🔒 **[GDPR-ready](./doc/time_variance.md#data-purging)**: Complete data excision for regulatory compliance
- 🚀 **[Production-proven](https://gitlab.com/arbetsformedlingen/taxonomy-dev)**: Tested with billions of datoms, deployed in government services

**Distributed by design**: Datahike is part of the [replikativ](https://github.com/replikativ) ecosystem for decentralized data architectures.

## Why Datalog?

Modern applications model increasingly complex relationships—social networks, organizational hierarchies, supply chains, knowledge graphs. Traditional SQL forces you to express graph queries through explicit joins, accumulating complexity as relationships grow. Datalog uses **pattern matching over relationships**: describe what you're looking for, not how to join tables.

As systems evolve, SQL schemas accumulate join complexity. What starts as simple tables becomes nested subqueries and ad-hoc graph features. Datalog treats relationships as first-class: transitive queries, recursive rules, and multi-database joins are natural to express. The result is maintainable queries that scale with relationship complexity. See [Why Datalog?](./doc/datalog-vs-sql.md) for detailed comparisons.

**Time is fundamental to information**: Most value derives from how facts evolve over time. Datahike's immutable design treats the database as an append-only log of facts—queryable at any point in history, enabling audit trails, debugging through time-travel, and GDPR-compliant data excision. Immutability also powers Distributed Index Space: database snapshots are values that can be shared, cached, and queried without locks.

You can find [API documentation on cljdoc](https://cljdoc.org/d/org.replikativ/datahike) and articles on Datahike on our company's [blog page](https://datahike.io/notes/).

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/org.replikativ/datahike)

We presented Datahike also at meetups,for example at:

- [2021 Bay Area Clojure meetup](https://www.youtube.com/watch?v=GG-S-xrDS5M)
- [2019 scicloj online meetup](https://www.youtube.com/watch?v=Hjo4TEV81sQ).
- [2019 Vancouver Meetup](https://www.youtube.com/watch?v=A2CZwOHOb6U).
- [2018 Dutch clojure meetup](https://www.youtube.com/watch?v=W6Z1mkvqp3g).

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/org.replikativ/datahike/latest-version.svg)](http://clojars.org/org.replikativ/datahike)

We provide a stable API for the JVM that we extend by first providing experimental/beta features that then get merged into the API over time.

```clojure
(require '[datahike.api :as d])


;; use the filesystem as storage medium
(def cfg {:store {:backend :file 
                  :id #uuid "550e8400-e29b-41d4-a716-446655440000"
                  :path "/tmp/example"}})

;; create a database at this place, per default configuration we enforce a strict
;; schema and keep all historical data
(d/create-database cfg)

(def conn (d/connect cfg))

;; the first transaction will be the schema we are using
;; you may also add this within database creation by adding :initial-tx
;; to the configuration
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one }
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one }])

;; lets add some data and wait for the transaction
(d/transact conn [{:name  "Alice", :age   20 }
                  {:name  "Bob", :age   30 }
                  {:name  "Charlie", :age   40 }
                  {:age 15 }])

;; search the data
(d/q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
  @conn)
;; => #{[3 "Alice" 20] [4 "Bob" 30] [5 "Charlie" 40]}

;; add new entity data using a hash map
(d/transact conn {:tx-data [{:db/id 3 :age 25}]})

;; if you want to work with queries like in
;; https://grishaev.me/en/datomic-query/,
;; you may use a hashmap
(d/q {:query '{:find [?e ?n ?a ]
               :where [[?e :name ?n]
                       [?e :age ?a]]}
      :args [@conn]})
;; => #{[5 "Charlie" 40] [4 "Bob" 30] [3 "Alice" 25]}

;; query the history of the data
(d/q '[:find ?a
       :where
       [?e :name "Alice"]
       [?e :age ?a]]
  (d/history @conn))
;; => #{[20] [25]}

;; you might need to release the connection for specific stores
(d/release conn)

;; clean up the database if it is not need any more
(d/delete-database cfg)
```

The API namespace provides compatibility to a subset of Datomic functionality
and should work as a drop-in replacement on the JVM. The rest of Datahike will
be ported to core.async to coordinate IO in a platform-neutral manner.

## Documentation

**[📖 Complete Documentation Index](./doc/README.md)** - Organized by topic and skill level

**Quick links:**
- [Configuration](./doc/config.md) - Database setup and backend options
- [Why Datalog?](./doc/datalog-vs-sql.md) - Query comparisons and when to use Datalog
- [Language Bindings](./doc/README.md#language-bindings-beta) - Java, JavaScript, Python, CLI, and more (beta)
- [Time Variance](./doc/time_variance.md) - Time-travel queries and GDPR-compliant purging


For simple examples have a look at the projects in the `examples` folder.

## Example Projects

### Applications

- **[Beleg](https://github.com/replikativ/beleg)** - Invoice and CRM system with web UI, LaTeX PDF generation, and Datahike persistence. Works as both an example project demonstrating Datahike patterns and a practical solution for contractors and small businesses. Features customers, tasks, offers, and invoices. Successor to the [original Datahike invoice demo](https://www.youtube.com/watch?v=W6Z1mkvqp3g) from Dutch Clojure Meetup 2018.

## ClojureScript & JavaScript Support

Datahike has **beta ClojureScript support** for both **Node.js** (file backend) and **browsers** (IndexedDB with TieredStore for memory hierarchies).

**JavaScript API** (beta):

Install from npm:
```bash
npm install datahike@next
```

Example usage:
```javascript
const d = require('datahike');
const crypto = require('crypto');

const config = {
  store: {
    backend: ':memory',
    id: crypto.randomUUID()
  },
  'schema-flexibility': ':read'
};

await d.createDatabase(config);
const conn = await d.connect(config);
await d.transact(conn, [{ name: 'Alice' }]);
const db = await d.db(conn);
const results = await d.q('[:find ?n :where [?e :name ?n]]', db);
console.log(results);
// => [['Alice']]
```

See [JavaScript API documentation](./doc/javascript-api.md) for details.

**Browser with real-time sync**: Combine IndexedDB storage with [Kabel](https://github.com/replikativ/kabel) WebSocket middleware for offline-capable applications.

**Native CLI tool** (`dthk`) (beta): Compiled with GraalVM native-image for instant startup. Ships with file backend support, scriptable for quick queries and automation. Available in [releases](https://github.com/replikativ/datahike/releases). See [CLI documentation](./doc/cli.md).

**Babashka pod** (beta): Native-compiled pod available in the [Babashka pod registry](https://github.com/babashka/pod-registry) for shell scripting. See [Babashka pod documentation](./doc/bb-pod.md).

**Java API** (beta): Comprehensive bindings with fluent builder pattern and automatic collection conversion. See [Java API documentation](./doc/java-api.md) for the full API guide and [examples](./examples/java/).

**libdatahike** (beta): *C/C++ native bindings* enable embedding Datahike in non-JVM applications. See [libdatahike documentation](./doc/libdatahike.md).

**Python bindings** (beta): High-level Pythonic API with automatic EDN conversion. See [Python documentation](./pydatahike/README.md).

## Production Use

### Swedish Public Employment Service

The [Swedish Public Employment Service](https://arbetsformedlingen.se) (Arbetsförmedlingen) has been using Datahike in production since 2024 to serve the [JobTech Taxonomy](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/jobtech-taxonomy-api) (Arbetsmarknadstaxonomin) - a labour market terminology database with 40,000+ concepts representing occupations, skills, and education standards, accessed daily by thousands of case workers across Sweden.

**Technical Highlights**:
- **Scale**: 60+ schema attributes with multi-language support (Swedish, English)
- **Architecture**: Multi-backend abstraction supporting both Datomic and Datahike
- **API**: GraphQL interface with Apache Lucene full-text search
- **Compliance**: Full transaction history for regulatory audit trail
- **Resilience**: S3-based backup/restore for disaster recovery
- **Migration**: Successfully migrated from Datomic after extensive testing (U1 → I1 → Production)

**Resources**:
- **Source Code**: [jobtech-taxonomy-api](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/jobtech-taxonomy-api) (2,851+ commits)
- **Benchmarks**: [Performance comparison suite](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark) (Datahike vs Datomic)
- **Migration Story**: [Plan.md](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/jobtech-taxonomy-api/-/blob/develop/test/datahike/Plan.md) - detailed deployment journey

This represents one of the most comprehensive open-source Datahike deployments, demonstrating production-readiness at government scale.

### Stub - Accounting for African Entrepreneurs

[Stub](https://stub.africa/) is a comprehensive accounting and invoicing platform serving 5,000+ small businesses across South Africa. Built by [Alexander Oloo](https://github.com/alekcz) with Datahike powering the core data layer.

**Features**: Invoicing with payment integration, double-entry bookkeeping, bank sync (Capitec, FNB, Absa, Nedbank), VAT tracking, inventory management, and financial reporting.

### Heidelberg University - Emotion Tracking

Heidelberg University uses Datahike in an internal emotion tracking application for psychological research (source not publicly available).

## Proximum: Vector Search for Datahike

[Proximum](https://datahike.io/proximum) is a high-performance HNSW vector index designed for Datahike's persistent data model. It brings semantic search and RAG capabilities to Datahike while maintaining immutability and full audit history.

**Key features**:
- Fast HNSW (Hierarchical Navigable Small World) vector search
- Immutable index snapshots—same git-like semantics as Datahike
- Persistent data structures without mutation or locks
- Dual-licensed: EPL-2.0 (open source) and commercial license

See [datahike.io/proximum](https://datahike.io/proximum) for details. Integration as secondary index into Datahike coming soon.

## Composable Ecosystem

Datahike is **compositional by design**—built from independent, reusable libraries that work together but can be used separately in your own systems. Each component is open source and maintained as part of the [replikativ](https://github.com/replikativ) project.

**Core libraries:**
- **[konserve](https://github.com/replikativ/konserve)**: Pluggable key-value store abstraction with backends for File, LMDB, S3, JDBC, Redis, IndexedDB, and more. Use it for any persistent storage needs beyond Datahike.
- **[kabel](https://github.com/replikativ/kabel)**: WebSocket transport with middleware support. Build real-time communication layers for any application.
- **[hasch](https://github.com/replikativ/hasch)**: Content-addressable hashing for Clojure data structures. Create immutable references to data.
- **[incognito](https://github.com/replikativ/incognito)**: Extensible serialization for custom types. Serialize any Clojure data across networks or storage.
- **[superv.async](https://github.com/replikativ/superv.async)**: Supervision and error handling for core.async. Build robust asynchronous systems.

**Advanced:**
- **[replikativ](https://github.com/replikativ/replikativ)**: CRDT-based data synchronization for eventually consistent systems. Build collaborative applications with automatic conflict resolution.
- **[distributed-scope](https://github.com/simm-is/distributed-scope)**: Remote function invocation with Clojure semantics across processes.

This modularity enables **custom solutions** across languages and runtimes: embed konserve in Python applications, use kabel for non-database real-time systems, or build entirely new databases on the same storage layer. Datahike demonstrates how these components work together, but you're not locked into our choices.

## Roadmap and Participation

Instead of providing a static roadmap, we work closely with the community to decide what will be worked on next in a dynamic and interactive way.

**How it works:**

Go to [GitHub Discussions](https://github.com/replikativ/datahike/discussions/categories/ideas) and upvote the _ideas_ you'd like to see in Datahike. When we have capacity for a new feature, we address the most upvoted items.

You can also propose ideas yourself—either by adding them to Discussions or by creating a pull request. Note that due to backward compatibility considerations, some PRs may take time to integrate.


## Commercial Support

We are happy to provide commercial support. If you are interested in a particular
feature, please contact us at [contact@datahike.io](mailto:contact@datahike.io).

## License

Copyright © 2014–2026 Christian Weilbach et al.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
