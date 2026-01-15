<p align="center">
  <a align="center" href="https://datahike.io" target="_blank">
    <img alt="Datahike" src="./doc/assets/datahike-logo.svg" height="128em">
  </a>
</p>
<p align="center">
<a href="https://clojurians.slack.com/archives/CB7GJAN0L"><img src="https://badgen.net/badge/-/slack?icon=slack&label"/></a>
<a href="https://clojars.org/io.replikativ/datahike"> <img src="https://img.shields.io/clojars/v/io.replikativ/datahike.svg" /></a>
<a href="https://circleci.com/gh/replikativ/datahike"><img src="https://circleci.com/gh/replikativ/datahike.svg?style=shield"/></a>
<a href="https://github.com/replikativ/datahike/tree/main"><img src="https://img.shields.io/github/last-commit/replikativ/datahike/main"/></a>
</p>

**Branch databases, not just code.**

[Datahike](https://datahike.io) is a durable [Datalog](https://en.wikipedia.org/wiki/Datalog) database with
Datomic-compatible APIs and git-like semantics. Built on persistent data structures and structural sharing,
database snapshots are immutable values that can be held, shared, and queried anywhere—without locks or copying.

**Key capabilities:**
- 🌐 **Distributed Index Space**: Read scaling without database connections—readers access persistent indices directly
- 🗄️ **Flexible storage**: File, LMDB, S3, JDBC, Redis, IndexedDB via [konserve](https://github.com/replikativ/konserve)—[choose what fits](./doc/storage-backends.md)
- 🌍 **Cross-platform**: JVM, Node.js, Browser (Clojure, ClojureScript, JavaScript, Java APIs)
- ⚡ **Real-time sync**: WebSocket streaming with [Kabel](https://github.com/replikativ/kabel) for browser ↔ server
- 🕰️ **Time-travel**: Query any historical state, full transaction audit trail ([versioning API](./doc/versioning.md) becoming stable)
- 🔒 **GDPR-ready**: Complete data excision for regulatory compliance
- 🚀 **Production-proven**: Tested with billions of datoms, [deployed in government services](https://gitlab.com/arbetsformedlingen/taxonomy-dev)

**Distributed by design**: Datahike is part of the [replikativ](https://github.com/replikativ) ecosystem for decentralized data architectures.

## Why Datalog?

Modern applications model increasingly complex relationships—social networks, organizational hierarchies, supply chains, knowledge graphs. Traditional SQL forces you to express graph queries through explicit joins, accumulating complexity as relationships grow. Datalog uses **pattern matching over relationships**: describe what you're looking for, not how to join tables.

As systems evolve, SQL schemas accumulate join complexity. What starts as simple tables becomes nested subqueries and ad-hoc graph features. Datalog treats relationships as first-class: transitive queries, recursive rules, and multi-database joins are natural to express. The result is maintainable queries that scale with relationship complexity. See [Why Datalog?](./doc/datalog-vs-sql.md) for detailed comparisons.

**Time is fundamental to information**: Most value derives from how facts evolve over time. Datahike's immutable design treats the database as an append-only log of facts—queryable at any point in history, enabling audit trails, debugging through time-travel, and GDPR-compliant data excision. Immutability also powers Distributed Index Space: database snapshots are values that can be shared, cached, and queried without locks.

You can find [API documentation on cljdoc](https://cljdoc.org/d/io.replikativ/datahike) and articles on Datahike on our company's [blog page](https://datahike.io/notes/).

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/io.replikativ/datahike)

We presented Datahike also at meetups,for example at:

- [2021 Bay Area Clojure meetup](https://www.youtube.com/watch?v=GG-S-xrDS5M)
- [2019 scicloj online meetup](https://www.youtube.com/watch?v=Hjo4TEV81sQ).
- [2019 Vancouver Meetup](https://www.youtube.com/watch?v=A2CZwOHOb6U).
- [2018 Dutch clojure meetup](https://www.youtube.com/watch?v=W6Z1mkvqp3g).

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/datahike/latest-version.svg)](http://clojars.org/io.replikativ/datahike)

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

Refer to the docs for more information:

- [Why Datalog?](./doc/datalog-vs-sql.md) - Query comparisons and when to use Datalog
- [Storage backends](./doc/storage-backends.md) - choosing the right backend for your needs
- [Distributed architecture](./doc/distributed.md) - Distributed Index Space and real-time sync
- [Versioning](./doc/versioning.md) - git-like branching and merging (beta)
- [Norms](./doc/norms.md) - database migration system
- [Configuration](./doc/config.md)
- [Schema flexibility](./doc/schema.md)
- [Time Variance](./doc/time_variance.md) - time-travel queries (as-of, history, since), audit trails, and GDPR-compliant purging
- [Garbage Collection](./doc/gc.md) - reclaim storage by removing old database snapshots
- [JavaScript API](./doc/javascript-api.md) - Promise-based API for Node.js and browsers
- [CLI](./doc/cli.md) - native command-line tool (dthk)
- [Babashka pod](./doc/bb-pod.md) - shell scripting with Datahike
- [libdatahike](./doc/libdatahike.md) - C/C++ native library
- [Benchmarking](./doc/benchmarking.md)
- [Differences to Datomic](./doc/datomic_differences.md)
- [Entity spec](./doc/entity_spec.md)
- [Logging and error handling](./doc/logging_and_error_handling.md)
- [Unstructured input support](./doc/unstructured.md) (experimental)
- [Backend development](./doc/backend-development.md)
- [Contributing to Datahike](./doc/contributing.md)


For simple examples have a look at the projects in the `examples` folder.

## Example Projects

- [Invoice creation](https://gitlab.com/replikativ/datahike-invoice)
  demonstrated at the [Dutch Clojure
  Meetup](https://www.meetup.com/de-DE/The-Dutch-Clojure-Meetup/events/trmqnpyxjbrb/).

## ClojureScript & JavaScript Support

Datahike has **beta ClojureScript support** for both **Node.js** (file backend) and **browsers** (IndexedDB with TieredStore for memory hierarchies).

**JavaScript API** (Promise-based):
```javascript
const d = require('datahike');
const crypto = require('crypto');

const config = {
  store: {
    backend: ':memory',
    id: crypto.randomUUID()
  },
  'schema-flexibility': ':read'  // Allow schemaless data (use kebab-case)
};

await d.createDatabase(config);
const conn = await d.connect(config);
await d.transact(conn, [{ name: 'Alice' }]);
const db = await d.db(conn);  // db() is async for async backends
const results = await d.q('[:find ?n :where [?e :name ?n]]', db);
console.log(results);
// => [['Alice']]
```

**Browser with real-time sync**: Combine IndexedDB storage with [Kabel](https://github.com/replikativ/kabel) WebSocket middleware for offline-capable applications that sync to server when online.

See [JavaScript API documentation](./doc/javascript-api.md) for details.

**npm package** (preview):
```bash
npm install datahike@next
```

**Native CLI tool** (`dthk`): Compiled with GraalVM native-image for instant startup. Ships with file backend support, scriptable for quick queries and automation. Available in [releases](https://github.com/replikativ/datahike/releases). See [CLI documentation](./doc/cli.md).

**Babashka pod**: Native-compiled pod available in the [Babashka pod registry](https://github.com/babashka/pod-registry) for shell scripting. See [Babashka pod documentation](./doc/bb-pod.md).

**Java API**: Shipped with the jar file is [Datahike.java](https://github.com/replikativ/datahike/blob/main/java/src/datahike/java/Datahike.java) (beta).

**libdatahike**: *C++ bindings* enable embedding Datahike in non-JVM applications. See [libdatahike documentation](./doc/libdatahike.md).

**Python bindings** (experimental): [pydatahike](https://github.com/replikativ/pydatahike). Please reach out if you have interest and we can make them work for you.

## Production Use

### Swedish Public Employment Service

The [Swedish Public Employment Service](https://arbetsformedlingen.se) (Arbetsförmedlingen) has been using Datahike in production since 2024 to store and serve the [Labour Market Taxonomy](https://gitlab.com/arbetsformedlingen/taxonomy-dev) (Arbetsmarknadstaxonomin). This is a terminology consisting of more than 40,000 labour market concepts, primarily representing occupations and skills, used to encode labour market data both within Arbetsförmedlingen and externally.

**Key facts**:
- **Scale**: 40,000+ concepts with complex relationships
- **Usage**: Thousands of case workers access the taxonomy API daily across Sweden
- **Versioning**: Transaction history provides full audit trail for regulatory compliance
- **Updates**: Continuously maintained to reflect current labour market
- **Open source**: [API source code](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/jobtech-taxonomy-api) and [benchmark suite](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/) are publicly available

**Benchmarks**: The Swedish government published [performance benchmarks](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/) comparing Datahike to Datomic across a range of complex queries representative of real-world government workloads.

## Proximum: Vector Search for Datahike

**Coming soon**: [Proximum](https://datahike.io/proximum) is a high-performance HNSW vector index designed for Datahike's persistent data model. It brings semantic search and RAG capabilities to Datahike while maintaining immutability and full audit history.

**Key features** (upcoming):
- Fast HNSW (Hierarchical Navigable Small World) vector search
- Immutable index snapshots—same git-like semantics as Datahike
- Persistent data structures without mutation or locks
- Dual-licensed: EPL-2.0 (open source) and commercial license

See [datahike.io/proximum](https://datahike.io/proximum) for details. Publication pending completion of current work.

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
