# Versioning: Branch Databases, Not Just Code

**Status: Beta - API becoming stable. Please try it out and provide feedback at [contact@datahike.io](mailto:contact@datahike.io).**

Datahike's versioning system brings **git-like branching and merging** to your database. Just as git lets you experiment with code changes in branches before merging, Datahike lets you branch entire databases, evolve them independently, and selectively merge changes back.

## Why Branch Databases?

**Structural sharing makes branching efficient.** Unlike copying entire databases, Datahike branches share unchanged data through persistent data structures—the same principle that makes git fast. Creating a branch is nearly instantaneous regardless of database size, because only new or modified index nodes are written.

When you create a branch, you get:
- **Isolated evolution**: Experiment without affecting production data
- **Selective merging**: Choose exactly which changes to apply
- **Zero data duplication**: Shared data exists only once in storage
- **Git-like semantics**: Branch, commit, merge with familiar concepts

## When to Use Branches vs. Separate Databases

**Use branches when:**
- Testing schema migrations before applying to production
- Creating staging environments for data review and approval
- Running what-if analyses or experiments
- Collaborative editing where changes need review before merging
- You need to evolve a single logical dataset through different paths

**Use separate databases when:**
- Data is logically independent (different customers, different projects)
- You want complete isolation with no shared storage
- Combining data from multiple sources (data federation)
- Scaling reads across completely separate datasets

## How Branches Work with Distributed Index Space

Branches are implemented as **different root pointers** in the same storage backend. Each branch name (`:db`, `:staging`, `:experimental`) points to a different commit, but all branches share the underlying persistent indices.

This means:
- Multiple readers can access different branches simultaneously via [DIS](distributed.md)
- No coordination needed between readers on different branches
- Branches can be accessed from any process with storage access
- Each branch maintains its own transaction history

## Relationship to Time-Travel Queries

Datahike provides two complementary ways to work with history:

**Versioning (branches):**
- Creates durable, named snapshots that evolve independently
- Allows merging changes between snapshots
- Permanent until explicitly deleted
- Use for: experiments, staging, alternative versions

**Time-travel queries (as-of, history, since):**
- Views past states of a single branch
- Read-only access to transaction history
- Automatic if `:keep-history? true`
- Use for: auditing, debugging, temporal queries

Both rely on the same persistent data structures, but serve different purposes.

## API Overview

The versioning API provides the following operations:

- `branch!` - Create a new branch from an existing branch
- `merge!` - Merge changes from one or more branches
- `force-branch!` - Create a branch from any in-memory DB value
- `delete-branch!` - Remove a branch
- `branch-history` - View commit history for a branch
- `commit-as-db` - Load a specific commit as a DB value
- `branch-as-db` - Load the current state of a branch
- `parent-commit-ids` - Get parent commits (for merge commits)

All operations work with the connection's configured storage backend—no special setup required.

## Example Use Cases

### Testing Schema Migrations

```clojure
(require '[datahike.api :as d]
         '[datahike.experimental.versioning :refer [branch! merge! delete-branch!]])

(let [cfg {:store {:backend :file :path "/var/db/production"}
           :keep-history? true
           :schema-flexibility :write}
      conn (d/connect cfg)]

  ;; Create migration test branch
  (branch! conn :db :migration-test)
  (let [test-conn (d/connect (assoc cfg :branch :migration-test))]

    ;; Try new schema
    (d/transact test-conn [{:db/ident :email
                            :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique :db.unique/identity}])

    ;; Test with sample data
    (d/transact test-conn [{:email "test@example.com"}])

    ;; Verify migration worked, then merge to production
    (when (verify-migration test-conn)
      (merge! conn #{:migration-test} (migration-tx-data test-conn))
      (delete-branch! conn :migration-test))))
```

### Staging Environment for Data Review

```clojure
;; Editorial workflow: draft changes in staging, review, then publish

(let [cfg {:store {:backend :s3 :bucket "my-content-db"}
           :schema-flexibility :write}
      prod-conn (d/connect cfg)]

  ;; Editor creates staging branch
  (branch! prod-conn :db :staging)
  (let [staging-conn (d/connect (assoc cfg :branch :staging))]

    ;; Make draft changes
    (d/transact staging-conn [{:article/title "New Article"
                               :article/status :draft
                               :article/content "..."}])

    ;; Reviewers can read staging branch without affecting production
    ;; ... review process ...

    ;; Approved? Merge to production
    (let [approved-changes (extract-approved-changes staging-conn)]
      (merge! prod-conn #{:staging} approved-changes))))
```

### Running Experiments

```clojure
;; Test different recommendation algorithms without affecting live data

(let [cfg {:store {:backend :file :path "/var/db/recommendations"}
           :keep-history? false} ;; Don't need history for experiments
      conn (d/connect cfg)]

  ;; Create experimental branch
  (branch! conn :db :experiment-new-algo)
  (let [exp-conn (d/connect (assoc cfg :branch :experiment-new-algo))]

    ;; Load experimental algorithm results
    (d/transact exp-conn experimental-recommendations)

    ;; Analyze results
    (let [metrics (analyze-recommendations @exp-conn)]
      (if (better-than-baseline? metrics)
        ;; Good results - merge to production
        (merge! conn #{:experiment-new-algo} experimental-recommendations)
        ;; Poor results - just delete the branch
        (delete-branch! conn :experiment-new-algo)))))
```

## Complete API Example

The following example demonstrates the full versioning API:

~~~clojure
(require '[superv.async :refer [<?? S]]
         '[datahike.api :as d]
         '[datahike.experimental.versioning :refer [branch! branch-history delete-branch! force-branch! merge!
                                                    branch-as-db commit-as-db parent-commit-ids]])

(let [cfg    {:store              {:backend :file
                                   :path    "/tmp/dh-versioning-test"}
              :keep-history?      true
              :schema-flexibility :write
              :index              :datahike.index/persistent-set}
      conn   (do
              (d/delete-database cfg)
              (d/create-database cfg)
              (d/connect cfg))
      schema [{:db/ident       :age
               :db/cardinality :db.cardinality/one
               :db/valueType   :db.type/long}]
      _      (d/transact conn schema)
      store  (:store @conn)]
  (branch! conn :db :foo) ;; new branch :foo, does not create new commit, just copies
  (let [foo-conn (d/connect (assoc cfg :branch :foo))] ;; connect to it
    (d/transact foo-conn [{:age 42}]) ;; transact some data
    ;; extracted data from foo by query
    ;; ...
    ;; and decide to merge it into :db
    (merge! conn #{:foo} [{:age 42}]))
  (count (parent-commit-ids @conn)) ;; => 2, as :db got merged from :foo and :db
  ;; check that the commit stored is the same db as conn
  (= (commit-as-db store (commit-id @conn)) (branch-as-db store :db) @conn) ;; => true
  (count (<?? S (branch-history conn))) ;; => 4 commits now on both branches
  (force-branch! @conn :foo2 #{:foo}) ;; put whatever DB value you have created in memory
  (delete-branch! conn :foo))
~~~

Here we create a database as usual, but then we create a branch `:foo`, write to
it and then merge it back. A simple query to extract all data in transactable
form that is in a `branch1` db but not in `branch2` is

~~~clojure
(d/q [:find ?db-add ?e ?a ?v ?t
      :in $ $2 ?db-add
      :where
      [$ ?e ?a ?v ?t]
      [(not= :db/txInstant ?a)]
      (not [$2 ?e ?a ?v ?t])]
      branch1 branch2 :db/add)
~~~

but you might want to be more selective when creating the data for `merge!`.

## Query Pattern: Extracting Branch Differences

When merging, you typically want to extract only specific changes from a branch. Here's a general pattern for finding differences:

```clojure
;; Find all datoms in branch1 that are not in branch2
(defn branch-diff [branch1 branch2]
  (d/q '[:find ?e ?a ?v ?t
         :in $ $2
         :where
         [$ ?e ?a ?v ?t]
         [(not= :db/txInstant ?a)]
         (not [$2 ?e ?a ?v ?t])]
       branch1 branch2))

;; Extract as transaction data
(defn diff-as-tx-data [branch1 branch2]
  (mapv (fn [[e a v t]] [:db/add e a v])
        (branch-diff branch1 branch2)))
```

You can extend this pattern to:
- Filter by specific attributes (e.g., only user-facing data)
- Extract only entities matching certain criteria
- Apply transformations before merging
- Validate changes before committing to the target branch

## Integration with Existing Connections

Branches integrate seamlessly with Datahike's connection model:

```clojure
;; Connect to specific branch by name
(def staging-conn (d/connect {:store {...} :branch :staging}))

;; Default branch is :db
(def main-conn (d/connect {:store {...}})) ;; same as :branch :db

;; Each connection operates independently
(d/transact staging-conn [...])  ;; doesn't affect main-conn
@staging-conn  ;; DB snapshot of :staging branch
@main-conn     ;; DB snapshot of :db branch
```

Branches work with all storage backends (file, S3, JDBC, etc.) and participate in [Distributed Index Space](distributed.md)—multiple processes can read different branches concurrently.

## Feedback and Support

We are actively developing the versioning API and would love to hear about your use cases. If you have ideas, feature requests, or encounter issues, please reach out to [contact@datahike.io](mailto:contact@datahike.io) or open an issue on [GitHub](https://github.com/replikativ/datahike/issues).
