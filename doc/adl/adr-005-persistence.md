# Changing the persistence layer

## Context
The persistent set has been proven to be a faster index than the hitchhiker tree index, but is only available for in-memory databases at this point of time. Looking into making it available to use with a backend to persist the database like the hitchhiker-tree allows us to do has uncovered some complexity in the current implementation of the persistence layer. Most striking is the need for the persistence via konserve store to require handlers for the (de-)serialization of structure elements. More concretely, here it causes the necessity to add hitchhiker-tree function `add-hitchhiker-tree-handlers` in multiple functions in `datahike.connector` that were meant to be independent of the type of index. This function also has to be called in all backend implementations, e.g. the filestore in `datahike.store`. (Note: The handlers should be moved out of the connector in any case and moved to the store namespace, as well as other konserve operations.)

Aside from that, the current implementation is also dependent of the tree structure of the index, so we could only maintain this way of doing things as long as a potential alternative index also has this structure.

The question now is if we keep the current way of implementing the persistence and simply adjust it to the persistent set or if we change it to make it easier to change the index structure or add new ones in the future.

## Options

### A: Keep the current structure

We would at least have to change the persistent set to mark dirty nodes and list them to change them in the backend. We could implement the handlers separate of the set, so we might be able to convince Tonsky to just let us do the small change. 

#### Pro

- datahike doesn't have to be changed
- loads only nodes that are accessed
- Write operations faster, only dirty nodes have to be saved
- persisted index structure could help later when multiple processes do changes here
  - tree nodes indicate if there were any changes

#### Contra

- additional library to maintain ourselves?
- have to get very familiar with persistent set implementation (Java)
- index structure not easily interchangeable
- additional space needed to persist tree nodes
- datoms persisted multiple times (`:eavt`, `:aevt`, `:vaet`)

### B: Persist db record as is without persisting each node separately

Instead of persisting each index, persist datom sequences for current and history data. Challenge then would be how to avoid writing nodes already there. A possible answer would be to record the operations done and replay them to change the backend store. Operations that cancel each other out could be skipped immediately.

#### Pro

- easy to use and maintain
- no need to change the persistent set -> but how to persist data structure????? 

#### Contra

- many more I/O operations on flush - might be much more expensive
- datoms persisted multiple times (`:eavt`, `:aevt`, `:vaet`)

### C: Persist datom sequences and replay ops for backends

Instead of persisting each index, persist datom sequences for current and history data. Challenge then would be how to avoid writing nodes already there. A possible answer would be to record the operations done and replay them to change the backend store. Operations that cancel each other out could be skipped immediately.

#### Pro

- independent of index structure -> modularity
- less than a third space required on disk 
- recording operations could be used for statistic

#### Contra

- have to be careful with implementation to not introduce new performance bottlenecks
- have to solve problem of how to separate between metadata (db record) and database values
- recording the operations could use a lot of RAM - restrict operations before (automatic) flushing?
- need to rebuild index structures on connect (expensive?)
- cannot be loaded piece by piece


## Status

**OPEN**

## Decision

**NONE**

## Consequences

  
