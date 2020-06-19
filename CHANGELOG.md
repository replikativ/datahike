# 0.3.1
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

# 0.3.0

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

# 0.2.1

- add numbers type
- re-introduce import/export functionality
- decouple backends from core
- integrate improved hitchhiker tree
- remove full eavt-index from db printing
- fix missing history entities

# 0.2.0

- integrate latest code from `datascript`
- move query parser to separate project: io.lambdaforge/datalog-parser
- add protocols for core indices: persistent set, hitchhiker tree now supported
- add protocols for backend stores: memory, file-based, LevelDB, PostgreSQL now
  supported (thanks to Alejandro Gómez)
- add schema-on-write capabilities
- add time variance capabilities
- add example project
- improve api documentation

# 0.1.3

- fixed null pointer exceptions in the compare relation of the hitchhiker-tree

# 0.1.2

- disk layout change, migration needed
- write root nodes of indices efficiently; reduces garbage by ~40 times and halves transaction times
- support export/import functionality

# 0.1.1

- preliminary support for datascript style schemas through create-database-with-schema
- support storage of BigDecimal and BigInteger values

# 0.1.0

- small, but stable JVM API
- caching for fast query performance in konserve
- reactive reflection warnings?
- schema support
- remove eavt-durable
- remove redundant slicing code
- generalize interface to indices
- integration factui/reactive?
