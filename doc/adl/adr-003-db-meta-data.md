# DB meta data

## Context

The database record `DB` has no information about its creation date, used versions and no unique identifier. See [discussion](https://github.com/replikativ/datahike/discussions/407). 

## Options

### A: Extension of `:config` in DB record

Add the addtional fields to the current `:config` field in the DB record:

#### Pro

- no refactoring of current record
- Database ID could be set from outside

#### Contra
- it's bad practice to have a user set internal database data
- meta data is semantically different from configuration

### B: Addtion of new field `meta` in DB record 

Create new field `:meta` in the DB record containing a hashmap with the attributes:

- `:datahike/version`: `String`
- `:datahike/created-at`: `Date`
- `:datahike/id`: `UUID`
- `:konserve/version`: `String`
- `:hitchhiker.tree/version`: `String`

#### Pro

- clean semantic 
- extensible

#### Contra

- extra implementation work
- migration might be needed

## Status

**OPEN**

## Decision

**NONE**

## Consequences

- older databases need to be migrated
