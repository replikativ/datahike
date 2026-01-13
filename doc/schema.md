# Schema

Datahike supports two approaches to data validation: **schema-on-read** (flexible, validate later) and **schema-on-write** (strict, validate immediately). Choose at database creation time - this cannot be changed later.

## When to Use Each Approach

| Aspect | Schema-on-read (`:read`) | Schema-on-write (`:write`, default) |
|--------|-------------------------|-------------------------------------|
| **Validation** | None - accepts any data | Enforces types and structure |
| **Use when** | Prototyping, evolving schemas, heterogeneous data | Production systems, data integrity critical |
| **Similar to** | MongoDB, Redis, JSON documents | PostgreSQL, Datomic, traditional RDBMS |
| **Trade-off** | Flexibility vs. runtime errors | Safety vs. schema migration complexity |

**Default:** Datahike uses `:write` (schema-on-write) by default since version 0.2.0.

## Schema-on-read

Schema-on-read accepts any data without validation. Set `:schema-flexibility :read` at database creation to enable this mode.

```clojure
(require '[datahike.api :as d])

(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440010"} :schema-flexibility :read})

(d/create-database cfg)

(def conn (d/connect cfg))

;; now you can add any arbitrary data
(d/transact conn {:tx-data [{:any "Data"}]})
```

## Schema-on-write

Schema-on-write (the default) enforces type safety and structure. Define your schema before adding data. The schema itself is stored in the database, so you transact it like any other data.

```clojure
(require '[datahike.api :as d])

;; since the :write approach is the default value we may also skip the setting
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440011"} :schema-flexibility :write})

(d/create-database cfg)

(def conn (d/connect cfg))

;; define a simple schema
(def schema [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

;; transact it
(d/transact conn {:tx-data schema})

;; now we can transact data based on the provided schema
(d/transact conn {:tx-data [{:name "Alice"}]})
```

The schema definition is for the most part compliant with Datomic's
[approach](https://docs.datomic.com/on-prem/schema.html). Required are three
attributes:

- `:db/ident`: the name of the attribute, defined as a keyword with optional
  namespace, e.g. `:user/name`
- `:db/valueType`: the type of the value associated with an attribute, e.g. `db.type/string`, see
  [below](#supported-value-types) for supported types
- `:db/cardinality`: the cardinality of the value, whether the value is a single
  value or a set of values, can be either `:db.cardinality/one` or `db.cardinality/many`

Additionally, the following optional attributes are supported:

- `db/doc`: the documentation for the attribute as a string
- `db/unique`: a uniqueness constraint on the attribute for a given value, can
  be either `db.unique/value` (only one entity with this attribute can have the same value) or `db.unique/identity`(only one entity can have
  the value for this attribute with upsert enabled)
- `db/index`: indicates whether an index for the attribute's value should be
  created as a boolean
- `db/isComponent`: indicates that an attribute of type `:db.type/ref` references a subcomponent of the entity that has the attribute (for cascading retractions)
- if `:db/valueType` is `:db.type/tuple`, one of:
  - `db/tupleAttrs`: a collection of attributes that make up the tuple (for [composite tuples](https://docs.datomic.com/on-prem/schema/schema.html#composite-tuples))
  - `db/tupleTypes`: a collection of 2-8 types that make up the tuple (for [heterogeneous fixed length tuples](https://docs.datomic.com/on-prem/schema/schema.html#heterogeneous-tuples))
  - `db/tupleType`: the type of the tuple elements (for [homogeneous variable length tuples](https://docs.datomic.com/on-prem/schema/schema.html#homogeneous-tuples))

### Supported value types

The following types are currently support in datahike:

| Value Type        | Corresponding Type   |
| ----------------- | -------------------- |
| `db.type/bigdec`  | java.math.BigDecimal |
| `db.type/bigint`  | java.math.BigInteger |
| `db.type/boolean` | Boolean              |
| `db.type/double`  | Double               |
| `db.type/float`   | Double or Float      |
| `db.type/instant` | java.util.Date       |
| `db.type/keyword` | clojure.lang.Keyword |
| `db.type/long`    | java.lang.Long       |
| `db.type/ref`     | java.lang.Long       |
| `db.type/string`  | String               |
| `db.type/symbol`  | clojure.lang.Symbol  |
| `db.type/uuid`    | java.util.UUID       |
| `db.type/tuple`   | clojure.lang.Vector  |

The schema is validated using [clojure.spec](https://clojure.org/guides/spec). For additional validation beyond schema, see [Entity Specs](entity_spec.md).

### Tuple Examples

Tuples store multiple values together as a single attribute. Datahike supports three tuple types:

#### Composite Tuples
Composite tuples combine existing attributes into a compound key:

```clojure
;; Define the component attributes
(def schema [{:db/ident :user/first-name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
             {:db/ident :user/last-name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
             ;; Composite tuple combining first and last name
             {:db/ident :user/full-name
              :db/valueType :db.type/tuple
              :db/tupleAttrs [:user/first-name :user/last-name]
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity}])

(d/transact conn {:tx-data schema})
(d/transact conn {:tx-data [{:user/first-name "Alice" :user/last-name "Smith"}]})

;; Query by the composite tuple
(d/q '[:find ?e :where [?e :user/full-name ["Alice" "Smith"]]] @conn)
```

#### Heterogeneous Fixed-Length Tuples
Fixed-length tuples with different types for each position:

```clojure
;; Geographic coordinate: [latitude longitude elevation]
(def schema [{:db/ident :location/coordinates
              :db/valueType :db.type/tuple
              :db/tupleTypes [:db.type/double :db.type/double :db.type/long]
              :db/cardinality :db.cardinality/one}])

(d/transact conn {:tx-data schema})
(d/transact conn {:tx-data [{:location/coordinates [37.7749 -122.4194 52]}]})
```

#### Homogeneous Variable-Length Tuples
Variable-length tuples where all elements have the same type:

```clojure
;; RGB color values
(def schema [{:db/ident :color/rgb
              :db/valueType :db.type/tuple
              :db/tupleType :db.type/long
              :db/cardinality :db.cardinality/one}])

(d/transact conn {:tx-data schema})
(d/transact conn {:tx-data [{:color/rgb [255 128 0]}]})
```

### Schema Migration

Updating existing schema is discouraged as it may cause data inconsistencies. Only updates to `:db/cardinality` and `:db/unique` are supported.

**Recommended migration strategies:**

1. **Add new attributes** - Create a new attribute and migrate data, deprecating the old one
2. **Export and reimport** - Export data, create new database with updated schema, transform and import data
3. **Use norms** - For systematic schema changes across environments, use [Norms (Database Migrations)](norms.md)

## Common Schema Patterns

### User Profile

```clojure
(def user-schema
  [{:db/ident :user/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Unique user identifier"}

   {:db/ident :user/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "User email address"}

   {:db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :user/roles
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many
    :db/doc "User roles (admin, editor, viewer)"}

   {:db/ident :user/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}])
```

### Blog Post with References

```clojure
(def blog-schema
  [{:db/ident :post/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}

   {:db/ident :post/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :post/content
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :post/author
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "Reference to user entity"}

   {:db/ident :post/tags
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}

   {:db/ident :post/published-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}])
```

### Hierarchical Data (Organization Chart)

```clojure
(def org-schema
  [{:db/ident :employee/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}

   {:db/ident :employee/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :employee/manager
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "Reference to manager (also an employee)"}

   {:db/ident :employee/department
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "Reference to department entity"}

   {:db/ident :department/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}])
```

### Component Entities (Cascading Deletion)

```clojure
(def order-schema
  [{:db/ident :order/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}

   {:db/ident :order/items
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true
    :db/doc "Line items are components - deleted when order is deleted"}

   {:db/ident :item/product-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :item/quantity
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident :item/price
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}])
```
