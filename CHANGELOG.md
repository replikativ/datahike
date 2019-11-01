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
