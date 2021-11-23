# Changelog

## 0.4.0

- Add attribute references (#211)
- Fix avet upsert (#308)
- Extend benchmarks
- Add byte array support
- Add search cache
- Fix lookup search (#335)
- Fix comparators (#328)
- Add search cache (#294)
- Allow schema attribute updates (thanks to @MrEbbinghaus)
- Fix hitchhiker-tree handling (#358)
- Improve pagination performance (#294)
- Improve upsert performance 
- Fix history duplicates (#363)
- Fix cardinality many duplicates (#364)
- Fix attribute translations
- Add config for index creation
- Remove uniqueness constraint for :db/txInstant
- Fix scalar binding for function output
- Fix equivalent datom input (#932)
- Fix load-entities bugs (#398, #400)
- Fix LRU cache (#404)
- Clean up code examples (#409)
- Add q as built-in (#412)
- Add meta data (#407)
- Add int? as built-in (#435)

## 0.3.6

- Add a generic remote transactor interface (#281)
- Improve and add more benchmarks (#307)
- Improve query engine performance by optimising hash joins (#306)
- Use the latest version of the hitchhiker tree which fixes an issue with comparators (#258)

## 0.3.5

- Fix a dependency issue with release v0.3.4.

## 0.3.4

- Fix issue with upsert operations not always executed in the right order
- Fix an issue with transactions on import
- Add more tests
- Improve benchmarks

## 0.3.3

- Support for tuples (#104)
- Switch to Clojure CLI tools (#253)
- Adapt API namespace for Datomic compatibility (#196)
- Implement query with string (#196)
- Implement transact with lazy sequence (#196, #78, #151)
- Change upsert implementation to improve transaction performance (#62)
- Improve [cljdoc](https://cljdoc.org/d/io.replikativ/datahike/) (#88)
- Format source code according to [Clojure Style Guide](https://github.com/bbatsov/clojure-style-guide) (#198)
- Improve benchmark tooling
- Improve documentation on the pull-api namespace

The improved api namespace is now the entry point to using Datahike and should be the only namespace that needs to be imported in your projects. However it is still possible to use other namespaces but there will be changes that might break existing behaviour. Please take a look at the [improved cljdoc documentation](https://cljdoc.org/d/io.replikativ/datahike/) for the api namespace.

With the change in the upsert implementation (#62), we expect up to 3x speedup in terms of transaction time. However, it also brings a breaking change to the content of transaction reports. In previous Datahike versions, following an upsert operation (which updates an existing entry), you would see in the :tx-data section of the transaction report both the old retracted datom and its newly added version. E.g.:

```clojure
#datahike.db.TxReport{
...
:tx-data [#datahike/Datom[1 :name "Ivan" 536870914 false]
          #datahike/Datom[1 :name "Petr" 536870914 true]]
...}
```

With this release, you would only see the newly added entry and no information about retraction or addition is shown (it is assumed to be an addition).

```clojure
#datahike.db.TxReport{
...
:tx-data [#datahike/Datom [1 :name "Petr" 536870914]]
...}
```

Thanks to all the contributors and the community for helping on this release. Special thanks go to [clojurists together](https://www.clojuriststogether.org/) for funding large parts of this work.

## 0.3.2

- added entity specs (#197)
- fixed hash computation (#190)
- improved printer (#202)
- fixed history upsert (#219)
- added database name to environ
- added circle ci orbs for ci/cd across all libraries (#167)
- fixed reverse schema update (#199)
- added automatic releases
- added benchmark utility
- extended time variance test
- updated dependencies
- adjusted documentation

## 0.3.1

- support returning maps (#149, #186)
- support on-write schema for empty-db (#178)
- add hashmap for transact! (#173)
- cleanup old benchmarks (#181)
- cleanup leftover code (#172)
- fix index selection (#143)
- fix in-memory database existence check (#180)
- improve API docs
- update dependencies
- use java 1.8 for release build

## 0.3.0

- overhaul configuration while still supporting the old one
- support of environment variables for configuration 
- added better default configuration
- adjust time points in history functions to match Datomic's API
- add load-entities capabilities
- add cas support for nil 
- add support for non-date tx attributes 
- add Java API
- add Java interop in queries
- add basic pagination
- add noHistory support
- multiple bugfixes including downstream dependencies

## 0.2.1

- add numbers type
- re-introduce import/export functionality
- decouple backends from core
- integrate improved hitchhiker tree
- remove full eavt-index from db printing
- fix missing history entities

## 0.2.0

- integrate latest code from `datascript`
- move query parser to separate project: io.lambdaforge/datalog-parser
- add protocols for core indices: persistent set, hitchhiker tree now supported
- add protocols for backend stores: memory, file-based, LevelDB, PostgreSQL now
  supported (thanks to Alejandro Gómez)
- add schema-on-write capabilities
- add time variance capabilities
- add example project
- improve api documentation

## 0.1.3

- fixed null pointer exceptions in the compare relation of the hitchhiker-tree

## 0.1.2

- disk layout change, migration needed
- write root nodes of indices efficiently; reduces garbage by ~40 times and halves transaction times
- support export/import functionality

## 0.1.1

- preliminary support for datascript style schemas through create-database-with-schema
- support storage of BigDecimal and BigInteger values

## 0.1.0

- small, but stable JVM API
- caching for fast query performance in konserve
- reactive reflection warnings?
- schema support
- remove eavt-durable
- remove redundant slicing code
- generalize interface to indices
- integration factui/reactive?
