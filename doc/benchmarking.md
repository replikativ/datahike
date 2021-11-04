# Benchmarking Datahike

There is a small command line utility integrated in this project to measure the performance of our *in-memory* and our *file* backend. It is also capable of comparing benchmarking results.

To run the benchmarks, navigate to the project folder in your console and run 
```bash
clj -M:benchmark CMD [OPTIONS] [FILEPATHS] 
```

The command can either be `run` or `compare`.


## Running Benchmarks

```bash
clj -M:benchmark run [OPTIONS] [OUTPUTFILEPATH]+
```

Options:

| Short | Long                      | Description                                                            | Default       |
|:-----:|---------------------------|------------------------------------------------------------------------|---------------|
| -u    | --db-server-url URL       | Base URL for datahike server for benchmark output.                     |               |
| -n    | --db-name DBNAME          | Database name for datahike server for benchmark output.                |               |
| -g    | --db-token TOKEN          | Token for datahike server for benchmark output.                        |               | 
| -t    | --tag TAG                 | Add tag to measurements; multiple tags possible.                       | `#{}`         | 
| -o    | --output-format FORMAT    | Short form of output format to use.                                    | `edn`         | 
| -c    | --config-name CONFIGNAME  | Name of database configuration to use.                                 | (all)         | 
| -d    | --db-entity-counts VECTOR | Numbers of entities in database for which benchmarks should be run.    | `[0 1000]`    | 
| -x    | --tx-entity-counts VECTOR | Numbers of entities in transaction for which benchmarks should be run. | `[0 1000]`    | 
| -y    | --data-types TYPEVECTOR   | Vector of data types to test queries on.                               | `[:int :str]` | 
| -z    | --data-found-opts OPTS    | Run query for existent or nonexistent values in the database.          | `:all`        | 
| -i    | --iterations ITERATIONS   | Number of iterations of each measurement.                              | `10`          | 
| -f    | --function FUNCTIONNAME   | Name of function to test.                                              | (all)         |
| -q    | --query QUERYNAME         | Name of query to test.                                                 | (all)         | 
| -h    | --help                    | Show help screen for tool usage.                                       |               |

### Examples

1) Benchmark *connection* time for databases with  *1000 entities* (= 4000 datoms) and *file* backend with tag *feature* and output as *edn*

```bash
TIMBRE_LEVEL=':warn' clj -M:benchmark run -f :connection -d '[1000]' -c file -t feature -o edn feature.edn
```

2) Benchmark *transaction* time for *integer* datoms and 10 entities (= 40 datoms) per transaction using *mem* backend and output as csv

```bash
TIMBRE_LEVEL=':warn' clj -M:benchmark run -f :transaction -y '[:int]' -x '[10]' -c mem-set -o csv feature.csv
```

3) Benchmark *query* time for a *simple query* for all backends, for every configuration run the query *10 times* and take the average time 

```bash
TIMBRE_LEVEL=':warn'  clj -M:benchmark run -f :query -q :simple-query -i 10
```

### Possible Options

#### Database Configurations  (-c)

Options for `-c`:
- `mem-set` for in-memory database with persistent-set index
- `mem-hht` for in-memory database with hitchhiker-tree index
- `file` for database with file store backend and hitchhiker-tree index

Implementations:

```clojure
(def db-configs
  [{:config-name "mem-set"
    :config {:store {:backend :mem :id "performance-set"}
             :schema-flexibility :write
             :keep-history? false
             :index :datahike.index/persistent-set}}
   {:config-name "mem-hht"
    :config {:store {:backend :mem :id "performance-hht"}
             :schema-flexibility :write
             :keep-history? false
             :index :datahike.index/hitchhiker-tree}}
   {:config-name "file"
    :config {:store {:backend :file :path "/tmp/performance-hht"}
             :schema-flexibility :write
             :keep-history? false
             :index :datahike.index/hitchhiker-tree}}])
```

#### Tested Functions (-f)

Options for `-f`:

- `:connection`: Testing `datahike/connect`. Run can be configured via options `-c`, `-d`, `-i`
- `:transaction`: Testing `datahike/transact`. Run can be configured via options `-c`, `-d`, `-x`, `-i`
- `:query`: Testing `datahike/q`. Run can be configured via options `-c`, `-d`, `-y`, `-z`, `-i`, `-q`
 
#### Data Types (-z)

Used for query functions

Options for `-z`:
- `:int` for datatype `long`
- `:str` for data type `String`

#### Queries (-q)

Options for `-q`: 

- Simple query: `:simple-query`
- Join queries: `:e-join-query` `:e-join-query-first-fixed` `:e-join-query-second-fixed` `:a-join-query` `:v-join-query`
- Predicate queries: `:equals-query` `:equals-query-1-fixed` `:less-than-query` `:less-than-query-1-fixed`
- Queries using arguments from bindings: `:scalar-arg-query` `:scalar-arg-query-with-join` `:vector-arg-query`
- Aggregate queries: `:stddev-query` `:variance-query` `:max-query` `:median-query` `:avg-query`
- Cache check queries: `:simple-query-first-run` `:simple-query-second-run`

If applicable, the queries are run for each different implemented data type, for data in the database and data not in the database.  

##### Simple Query 

Implementation:

```clojure
(defn simple-query [db attr val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr val))
   :args [db]})
```

##### Join Queries

Implementation:

```clojure
(defn e-join-query [db attr1 attr2]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr1 '?v1)
                (conj '[?e] attr2 '?v2))
   :args [db]})

(defn a-join-query [db attr]
  {:query (conj '[:find ?v1 ?v2 :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2))
   :args [db]})

(defn v-join-query [db attr1 attr2]
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr1 '?v)
                (conj '[?e2] attr2 '?v))
   :args [db]})

(defn e-join-query-first-fixed [db attr1 val1 attr2]
  {:query (conj '[:find ?v2 :where]
                (conj '[?e] attr1 val1)
                (conj '[?e] attr2 '?v2))
   :args [db]})

(defn e-join-query-second-fixed [db attr1 attr2 val2]
  {:query (conj '[:find ?v1 :where]
                (conj '[?e] attr1 '?v1)
                (conj '[?e] attr2 val2))
   :args [db]})
```

##### Predicate Queries

Implementation:

```clojure
(defn less-than-query [db attr]
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2)
                '[(< ?v1 ?v2)])
   :args [db]})

(defn equals-query [db attr]
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2)
                '[(= ?v1 ?v2)])
   :args [db]})

(defn less-than-query-1-fixed [db attr comp-val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr '?v)
                (conj '[]
                      (sequence (conj '[= ?v] comp-val))))
   :args [db]})

(defn equals-query-1-fixed [db attr comp-val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr '?v)
                (conj '[]
                      (sequence (conj '[= ?v] comp-val))))
   :args [db]})
```

##### Binding Queries 

Implementation:

```clojure
(defn scalar-arg-query [db attr val]
  {:query (conj '[:find ?e
                  :in $ ?v
                  :where]
                (conj '[?e] attr '?v))
   :args [db val]})

(defn scalar-arg-query-with-join [db attr val]
  {:query (conj '[:find ?e1 ?e2 ?v2
                  :in $ ?v1
                  :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2))
   :args [db val]})

(defn vector-arg-query [db attr vals]
  {:query (conj '[:find ?e
                  :in $ ?v
                  :where]
                (conj '[?e] attr '?v))
   :args [db vals]})
```

##### Aggregate Queries
Only run for data type `:int`.

```clojure
[{:function :sum-query
  :query {:query '[:find (sum ?x)
                   :in [?x ...]]
          :args [(repeatedly (count entities) #(rand-int 100))]}}

 {:function :avg-query
    :query {:query '[:find (avg ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}}

 {:function :median-query
  :query {:query '[:find (median ?x)
                     :in [?x ...]]
          :args [(repeatedly (count entities) #(rand-int 100))]}}
 {:function :variance-query
  :query {:query '[:find (variance ?x)
                   :in [?x ...]]
          :args [(repeatedly (count entities) #(rand-int 100))]}}

 {:function :stddev-query
  :query {:query '[:find (stddev ?x)
                   :in [?x ...]]
          :args [(repeatedly (count entities) #(rand-int 100))]}}

 {:function :max-query
  :query {:query '[:find (max ?x)
                   :in [?x ...]]
          :args [(repeatedly (count entities) #(rand-int 100))]}}]
```

##### Cache check queries

Simple query with exact same configuration run twice. Identifiers are `:simple-query-first-run` for the first time it is run and `:simple-query-second-run` for the second run.

### Output Configuration

*Formats:*
- `remote-db`; using an instance of datahike-server to upload the results. The server configuration iscontrolled by options -u -n and -g
- `edn`
- `csv`

If an output filename is given the result is saved in a file instead of printed to stdout.

The edn output will look as follows:

```clojure
[ ;; ...
 {:context {:dh-config {:schema-flexibility :write, 
                        :keep-history? false, 
                        :index :datahike.index/persistent-set, 
                        :name "mem-set", 
                        :backend :mem}, 
            :function :vector-arg-query, 
            :db-entities 2500, 
            :db-datoms 10000, 
            :execution {:data-type :int, 
                        :data-in-db? true}}, 
  :time {:mean 0.17954399999999998, 
         :median 0.172268,               
         :std 0.02388124449855995, 
         :count 10, 
         :observations [0.173015 0.168094 0.174449 0.250349 0.169847 0.168364 0.168926 0.169352 0.172268 0.180776]}, 
  :tag "bind-collection-bounds-opt"}
  ;; ...
]
```


## Comparing Benchmarks

Usage:
```bash
clj -A:benchmark compare [-p] [FILEPATHS]* 
```

The comparison tool gives the option to *compare any number of benchmarking results* using
a) textual *table* format (default) or
b) *plots* (if command line option `-p` has been given).

Please note: 
1) At the moment the comparison tool can only *handle edn files* as input, so be aware of that and run the benchmarks with `--output edn` when you are planning to use this tool later. 
2) Use *different tags* for the benchmarks you want to compare since the comparison tool assumes measurements with the same tag to belong to the same group of measurements.

Example for comparison table:

```bash
clj -A:benchmark compare benchmarks1.edn benchmarks2.edn
```

Example for comparison plots:

```bash
clj -A:benchmark compare -p benchmarks1.edn benchmarks2.edn
```

The plots produced are scatter plots of the results combined with line plots using the median of the values for a measurement point.

If you want to see plots for a single benchmarking result, nothing keeps you from using the comparison tool on a single file. 

### Measuring Performance Changes During Development

The comparison tool has proven valuable to our team for comparing different branches of the datahike project to detect performance regressions or improvements.

Workflow:

1) Run the benchmarks on the branch you are forking from with the option `--output edn` giving it an expressive tag, e.g. the name of the branch:
```bash
git checkout development
clj -A:benchmark run -t development -o edn development.edn
```

2) Run the benchmarks on your feature branch with the option `--output edn` giving it a tag, e.g. the name of your new feature:
```bash
git checkout feature
clj -A:benchmark run -t feature -o edn feature.edn
```

3) Run the comparison tool on any branch giving both of the previous output files as input:
```bash
clj -A:benchmark compare -p development.edn feature.edn
```