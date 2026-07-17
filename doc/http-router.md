# Datahike HTTP Router

The `datahike.http.router` namespace provides route generation and handlers for the Datahike HTTP API, allowing you to embed Datahike's HTTP interface in your own applications without pulling in server dependencies.

## Motivation

Previously, using Datahike's HTTP API required running the full HTTP server with all its dependencies (Jetty, Ring, etc.). This prevented:
- GraalVM native image compilation due to heavy server dependencies
- Embedding Datahike routes in existing applications
- Using alternative server implementations

The router namespace solves these issues by separating route definitions from server implementation.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Main Library                         │
│                                                           │
│  datahike.http.router                                    │
│  - Route generation from API spec                        │
│  - Handler functions                                     │
│  - Minimal middleware                                    │
│  - No server dependencies                                │
└─────────────────────────────────────────────────────────┘
                           │
                           │ Uses
                           ▼
┌─────────────────────────────────────────────────────────┐
│                    HTTP Server                           │
│                                                           │
│  datahike.http.server                                    │
│  - Full server implementation                            │
│  - Jetty, Ring, Reitit dependencies                      │
│  - Authentication, CORS, Swagger UI                      │
│  - Distributed mode support                              │
└─────────────────────────────────────────────────────────┘
```

## Usage Modes

### 1. Embedded Mode (With Authentication)

The standard way to embed Datahike routes in your application with proper authentication:

```clojure
(require '[datahike.http.router :as router])

;; Standard approach: With authentication (same config format as server!)
(def config
  {:token    "securerandompassword"  ; Required for production
   :dev-mode false                    ; Set to true only for development
   :level    :info})                  ; Optional logging level

(def handler
  (router/create-ring-handler
    :config config                    ; Pass the security config
    :prefix "/datahike"               ; Mount under /datahike (optional)
    :include-writers? false           ; Don't expose internal writer routes
    :middleware [your-middleware]))

;; Use with any Ring-compatible server
(run-jetty handler {:port 3000})

;; Connect from REPL with authentication (include prefix in URL)
(d/connect {:store {:backend :datahike-server
                    :url "http://localhost:3000/datahike"
                    :token "securerandompassword"}})
```

**Custom Prefix Examples:**
```clojure
;; Mount under /api/db
(def handler (router/create-ring-handler
               :config config
               :prefix "/api/db"))

;; Mount under /v1/database
(def handler (router/create-ring-handler
               :config config
               :prefix "/v1/database"))

;; No prefix (root level)
(def handler (router/create-ring-handler
               :config config))  ; :prefix defaults to ""
```

**Development Mode**: For local development only, you can bypass authentication:
```clojure
(def dev-config {:dev-mode true})  ; WARNING: Never use in production!

(def dev-handler
  (router/create-ring-handler :config dev-config))
```

### 2. Distributed Mode

Run the full HTTP server for distributed deployments:

```bash
# Run the standalone server
clojure -A:http-server -m datahike.http.server config.edn
```

## Complete Minimal Example

Save this as `server.clj` and run with `clojure server.clj`:

```clojure
(ns server
  (:require [datahike.api :as d]
            [datahike.http.router :as router]
            [ring.adapter.jetty :refer [run-jetty]]))

(def config
  {:token    "securerandompassword"  ; Required for production
   :dev-mode false})                  ; Set to true for local development only

(defn -main []
  ;; Create a test database for demo
  (d/create-database {:store {:backend :mem :id "demo"}})

  ;; Create handler with authentication and mount under /datahike
  (let [handler (router/create-ring-handler
                 :config config
                 :prefix "/datahike")]

    (println "\n=== Datahike Embedded Server ===")
    (println "URL: http://localhost:8080/datahike")
    (println "Auth: token required (see config)")
    (println "\nTest with:")
    (println "  curl -X GET http://localhost:8080/datahike/database-exists? \\")
    (println "       -H \"Authorization: token securerandompassword\" \\")
    (println "       -d '[{\"store\":{\"backend\":\"mem\",\"id\":\"demo\"}}]'")

    (run-jetty handler {:port 8080 :join? false})))

;; Connect from REPL
(comment
  ;; Start server
  (-main)

  ;; Connect with authentication (include prefix in URL)
  (def conn (d/connect {:store {:backend :datahike-server
                                 :url "http://localhost:8080/datahike"
                                 :token "securerandompassword"}}))

  ;; Use normally
  (d/transact conn [{:name "Alice"}])
  (d/q '[:find ?name :where [_ :name ?name]] @conn))
```

## API Functions

### `create-routes`

Main function for getting routes in various formats:

```clojure
(create-routes & {:keys [format prefix include-writers? middleware]})
```

Options:
- `:format` - Output format (`:raw`, `:reitit`, `:compojure`, `:ring`)
- `:prefix` - URL prefix for all routes (e.g., `"/datahike"` or `"/api/db"`)
- `:include-writers?` - Include internal writer routes (default `false`)
- `:middleware` - Additional middleware to apply

### `create-ring-handler`

Main function for creating a secured Ring handler:

```clojure
(create-ring-handler & {:keys [config prefix include-writers? middleware not-found-handler]})
```

**Required for Production:**
- `:config` - Security configuration (same format as server config):
  - `:token` - Authentication token **(required for production)**
  - `:dev-mode` - If true, bypasses authentication **(never use in production)**
  - `:level` - Log level (optional)

**Other Options:**
- `:prefix` - URL prefix for all routes (e.g., `"/datahike"` or `"/api/db"`)
- `:include-writers?` - Include internal writer routes (default `false` - keep it false for embedded mode)
- `:middleware` - Additional middleware to apply
- `:not-found-handler` - Handler for unmatched routes

**Standard Usage:**
```clojure
;; Production configuration with prefix
(def handler
  (router/create-ring-handler
    :config {:token "securerandompassword"
             :dev-mode false}
    :prefix "/datahike"))

;; Without prefix (routes at root)
(def handler
  (router/create-ring-handler
    :config {:token "securerandompassword"
             :dev-mode false}))
```

### `generate-api-routes` / `generate-writer-routes`

Lower-level functions that return raw route definitions:

```clojure
;; Get all API routes as data
(generate-api-routes)
;; => [{:path "/create-database" :method :post :handler fn ...} ...]

;; Get internal writer routes (for distributed mode)
(generate-writer-routes)
```

## Route Formats

### Raw Format
```clojure
{:path "/create-database"
 :method :post
 :handler #function[...]
 :name :create-database
 :doc "Creates a new database..."}
```

### Reitit Format
```clojure
["/create-database"
 {:post {:handler #function[...]
         :name :create-database
         :summary "Creates a new database"
         :middleware []}}]
```

### Ring Format
Returns a handler function that matches routes internally.

## Integration Examples

### With Existing Reitit App

```clojure
;; Standard: with authentication
(def config {:token "your-api-token" :dev-mode false})

(def app
  (ring/ring-handler
    (ring/router
      (concat
        ;; Your routes
        [["/health" {:get health-handler}]]
        ;; Secured Datahike routes
        (router/create-routes :format :reitit)))
    ;; Apply authentication at router level
    {:middleware [(fn [handler]
                    (router/wrap-token-auth handler config))]}))
```

### With Compojure

```clojure
;; Standard: with authentication
(def datahike-config {:token "your-api-token" :dev-mode false})

(defroutes app-routes
  ;; Your routes
  (GET "/" [] "Home")

  ;; Mount secured Datahike under /api
  (context "/api" []
    (let [handler (router/create-ring-handler :config datahike-config)]
      (fn [req] (handler req)))))
```

## Authentication

### Standard Token Authentication

The router uses token-based authentication that matches the Datahike server configuration format. This is the recommended approach for production deployments:

```clojure
;; Configuration (same format as server config.edn)
(def config
  {:token    "securerandompassword"  ; Required for production
   :dev-mode false                    ; Never true in production
   :level    :info})                  ; Optional logging

(def handler
  (router/create-ring-handler
    :config config
    :middleware [wrap-json-response]))

;; Clients must include the token
(d/connect {:store {:backend :datahike-server
                    :url "http://localhost:8080/datahike"
                    :token "securerandompassword"}})
```

**Supported Authentication Headers:**
- `Authorization: token <your-token>` (Datahike format)
- `Authorization: Bearer <your-token>` (OAuth/JWT compatible)

### Alternative: Custom Authentication

For specialized requirements (OAuth, JWT, mTLS, etc.), you can implement custom authentication:

```clojure
(defn wrap-custom-auth [handler]
  (fn [request]
    (if (valid-user? request)
      (handler request)
      {:status 401 :body "Unauthorized"})))

;; Use custom auth instead of built-in
(def handler
  (-> (router/create-ring-handler)  ; Omit :config for custom auth
      wrap-custom-auth
      wrap-json-response))
```

### Development Mode (Local Only)

For local development, you can temporarily disable authentication:

```clojure
;; WARNING: Never deploy with dev-mode enabled!
(def dev-handler
  (router/create-ring-handler
    :config {:dev-mode true}))
```

## GraalVM Native Image

The router namespace is compatible with GraalVM native image compilation:

1. Include only `datahike.http.router` in your app
2. Exclude HTTP server dependencies
3. Compile to native image:

```clojure
;; deps.edn
{:deps {io.replikativ/datahike {:mvn/version "..."}}
 ;; No :http-server alias needed for embedded mode

 :aliases
 {:native-image
  {:main-opts ["-m" "clj.native-image" "your.main"
               "--no-fallback"
               "--initialize-at-build-time"]}}}
```

## Migration Guide

### From Standalone Server to Embedded

Before (running separate server):
```bash
clojure -A:http-server -m datahike.http.server config.edn
```

After (embedded in your app):
```clojure
(ns your.app
  (:require [datahike.http.router :as router]
            [ring.adapter.jetty :refer [run-jetty]]))

(def handler
  (router/create-ring-handler
    :middleware [wrap-cors wrap-json]))

(defn -main []
  (run-jetty handler {:port 8080}))
```

### From Embedded to Distributed

Simply run the full server alongside your embedded app:

```bash
# Development: embedded mode
lein run

# Production: distributed mode
docker run -p 8080:8080 datahike/http-server
```

## Performance Considerations

- Route generation happens once at startup
- Handlers are pre-compiled, no runtime overhead
- Minimal middleware stack in embedded mode
- Full caching and optimization in distributed mode

## Security Best Practices

### Required for Production

1. **Always configure authentication:**
   ```clojure
   :config {:token "use-a-strong-random-token"
            :dev-mode false}  ; NEVER true in production
   ```

2. **Use environment variables for tokens:**
   ```clojure
   :config {:token (System/getenv "DATAHIKE_TOKEN")
            :dev-mode false}
   ```

3. **Never expose writer routes in embedded mode:**
   ```clojure
   :include-writers? false  ; Always false for embedded
   ```

4. **Use HTTPS in production:**
   - Deploy behind a reverse proxy with TLS termination
   - Never send tokens over unencrypted connections

5. **Rotate tokens regularly:**
   - Change authentication tokens periodically
   - Use different tokens for different environments

### Security Checklist

- [ ] Authentication token configured
- [ ] `:dev-mode` is `false`
- [ ] `:include-writers?` is `false`
- [ ] Token stored securely (env var or secret manager)
- [ ] HTTPS enabled in production
- [ ] Different tokens for dev/staging/production

## Troubleshooting

### Routes not working
- Check that body params are properly formatted
- Ensure content-type is set (application/json, application/edn)
- Verify middleware ordering

### GraalVM compilation fails
- Ensure no server dependencies in classpath
- Check for reflection warnings
- Add necessary reflection config

### Performance issues
- Use distributed mode for high load
- Add caching middleware in embedded mode
- Consider connection pooling