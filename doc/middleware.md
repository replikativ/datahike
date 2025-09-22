# Datahike Middleware System

Datahike provides a middleware system that allows you to intercept and modify database operations. Middleware functions can be applied to queries to add functionality like timing, logging, or custom processing.

## Overview

Middleware in Datahike works by wrapping handler functions with additional behavior. The middleware system is primarily used for query operations and is configured through the database configuration.

## Core Functions

### `apply-middlewares`

Located in `datahike.middleware.utils`, this function combines multiple middleware functions into a single composed function.

```clojure
(apply-middlewares middlewares handler)
```

Takes a vector of middleware function symbols and a handler function, returning a new function with all middlewares applied. Middleware is applied in the order specified.

## Configuration

Middleware is configured in the database configuration map under the `:middleware` key:

```clojure
{:store {:backend :mem :id "example"}
 :middleware {:query ['my.namespace/timing-middleware
                      'my.namespace/logging-middleware]}}
```

## Built-in Middleware

### `timed-query`

Located in `datahike.middleware.query`, this middleware logs timing information for queries.

```clojure
(defn timed-query [query-handler]
  (fn [query & inputs]
    ;; Executes query and prints timing information
    ))
```

Usage:
```clojure
{:middleware {:query ['datahike.middleware.query/timed-query]}}
```

This middleware prints:
- Query as string
- Query execution time
- Input arguments

## Writing Custom Middleware

Middleware functions follow a specific pattern:

```clojure
(defn my-middleware [handler]
  (fn [& args]
    ;; Pre-processing
    (let [result (apply handler args)]
      ;; Post-processing
      result)))
```

As a typical middleware you need to return the result at the end of your middleware to continue processing. Please take a look at the timed-query middleware where we only print the time used for the query and continue processing afterwards.

## Examples

See test files for working examples:
- `test/datahike/test/middleware/utils_test.cljc` - Basic middleware composition
- `test/datahike/test/middleware/query_test.cljc` - Query middleware usage

## Error Handling

Invalid middleware symbols will throw an exception during database connection. Middleware functions must exist and be resolvable at connection time.
