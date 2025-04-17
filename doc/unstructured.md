# Unstructured Input Support

This experimental feature enables automatic schema inference from unstructured data (like JSON/EDN), allowing you to store richly nested data in Datahike with minimal setup.

## Overview

The unstructured input feature automatically:

- Infers schema definitions from your data structure
- Handles nested objects, collections, and primitive values
- Converts complex data structures to Datahike's entity model
- Works with both schema-on-read and schema-on-write configurations

## Basic Usage

```clojure
(require '[datahike.api :as d])
(require '[datahike.experimental.unstructured :as du])

;; Initialize database (schema-on-read is best for unstructured data)
(def cfg {:store {:backend :mem :id "my-db"}
          :schema-flexibility :read})
(d/create-database cfg)
(def conn (d/connect cfg))

;; Insert complex unstructured data with a single function call
(du/transact-unstructured conn 
                        {:name "Alice"
                         :age 42
                         :addresses [{:city "New York" :primary true}
                                     {:city "London" :primary false}]
                         :skills ["programming" "design"]})
```

## Type and Cardinality Mapping

The feature maps runtime values to Datahike types according to this conversion:

| Input Type | Datahike Type | Cardinality |
|------------|---------------|-------------|
| Integer | `:db.type/long` | `:db.cardinality/one` |
| Float | `:db.type/float` | `:db.cardinality/one` |
| Double | `:db.type/double` | `:db.cardinality/one` |
| String | `:db.type/string` | `:db.cardinality/one` |
| Boolean | `:db.type/boolean` | `:db.cardinality/one` |
| Keyword | `:db.type/keyword` | `:db.cardinality/one` |
| Symbol | `:db.type/symbol` | `:db.cardinality/one` |
| UUID | `:db.type/uuid` | `:db.cardinality/one` |
| Date | `:db.type/instant` | `:db.cardinality/one` |
| Map | `:db.type/ref` | `:db.cardinality/one` |
| Vector/List (with values) | Based on first value | `:db.cardinality/many` |
| Empty Vector/List | No schema generated until values exist | - |

### Array Handling and Cardinality-Many

When you provide an array/vector value, the system automatically:

1. Infers the element type from the first item in the collection 
2. Sets the attribute's cardinality to `:db.cardinality/many`
3. Stores each value in the array as a separate datom

This means you can model one-to-many relationships naturally:

```clojure
;; This will create tags with cardinality/many
(du/transact-unstructured conn 
                          {:name "Alice"
                           :tags ["clojure" "database" "datalog"]})

;; Later you can add more tags without losing existing ones
(d/transact conn [{:db/id [:name "Alice"] 
                   :tags ["awesome"]}])

;; All tags will be preserved
;; => ["clojure" "database" "datalog" "awesome"]
```

## Advanced Usage

### Working with the Schema Directly

If you need more control, you can process the data first to examine the schema:

```clojure
;; Process data to get both schema and transaction data
(def result (du/process-unstructured-data 
              {:user {:name "Bob" 
                      :roles ["admin" "user"]}}))

;; Examine the inferred schema
(println (:schema result))
;; => [{:db/ident :user, :db/valueType :db.type/ref, :db/cardinality :db.cardinality/one}
;;     {:db/ident :name, :db/valueType :db.type/string, :db/cardinality :db.cardinality/one}
;;     {:db/ident :roles, :db/valueType :db.type/string, :db/cardinality :db.cardinality/many}]

;; Manually transact the schema first, if needed
(d/transact conn (:schema result))

;; Then transact the data
(d/transact conn (:tx-data result))
```

### Schema-on-Write Databases

For schema-on-write databases (the default), the feature automatically:

1. Infers schema definitions from your data
2. Adds any missing schema attributes in the same transaction
3. Validates compatibility with existing schema
4. Throws helpful errors if there's a conflict

This makes the API particularly powerful for schema-on-write - you get the benefits of schema validation without the manual work of defining schemas upfront.

```clojure
;; With a schema-on-write database
(def cfg {:store {:backend :mem :id "my-strict-db"}})
(d/create-database cfg) 
(def conn (d/connect cfg))

;; This will add schema and data in a single transaction
(du/transact-unstructured conn {:name "Charlie" :age 35})

;; Later, you can add entities with new attributes
;; The schema will automatically be extended
(du/transact-unstructured conn {:name "Diana" 
                                :age 28 
                                :email "diana@example.com"}) ;; New attribute!
```

The feature supports incremental schema evolution - as your data model grows, the schema grows with it.

### Schema Conflicts and Error Handling

When using `transact-unstructured` with schema-on-write databases, the feature automatically detects schema conflicts. For example, if you've defined `:score` as a number but try to insert it as a string:

```clojure
;; First, establish schema for score as a number
(d/transact conn [{:db/ident :score
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}])

;; Add a valid score
(d/transact conn [{:score 100}])

;; This will fail with a schema conflict error:
(du/transact-unstructured conn {:name "Bob", :score "High"})
;; => ExceptionInfo: Schema conflict detected with existing database schema
;;    {:conflicts [{:attr :score, :conflict :value-type, 
;;                 :existing :db.type/long, :inferred :db.type/string}]}
```

The error messages provide details about the specific conflicts, making it easy to diagnose and fix issues.

## Future Directions

This experimental feature will likely be enhanced with:

- Better handling of schema conflicts
- Schema upgrading for schema-on-write databases 
- Performance optimizations for large datasets
- Special handling for additional data types

## Limitations

- Complex querying across nested entities requires knowledge of how the data was transformed
- For schema-on-write, existing schema attributes must be compatible
- Very large nested data structures might require chunking (not yet implemented)