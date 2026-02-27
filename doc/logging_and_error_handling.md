# Logging and Error Handling

Datahike uses `org.replikativ/logging` for all logging. This library wraps
[taoensso/trove](https://github.com/taoensso/trove), a lightweight logging facade that lets
you bring your own backend (console, SLF4J/Logback, Timbre, Telemere, or any custom function).

As a library, Datahike does not force any logging implementation on users. By default, trove
logs to stdout via its built-in console backend. Your application can replace this with any
backend (Timbre, SLF4J, etc.) by calling `trove/set-log-fn!`.

## Structured Logging

Every log call in Datahike uses a **namespaced keyword ID** as its first argument, following
the `:datahike/component` pattern. This enables reliable filtering and analysis:

```clojure
(require '[replikativ.logging :as log])

;; ID + data map (preferred for variable data)
(log/info :datahike/connect {:config config})

;; ID + message string (for human-readable context)
(log/debug :datahike/writer-closed "Writer thread gracefully closed")

;; ID + message + data map
(log/warn :datahike/tx-queue-pressure "Queue >90% full" {:count n :size size})
```

## Configuring a Log Backend

Install a log backend via `taoensso.trove/set-log-fn!`. The function receives
`[ns coords level id lazy_]` on each log event.

### Console logging (simplest)

```clojure
(require '[taoensso.trove :as trove]
         '[taoensso.trove.console :as trove-console])

;; Log to stdout at :info level and above
(trove/set-log-fn! (trove-console/get-log-fn {:min-level :info}))
```

### Console logging to stderr

```clojure
(defn make-stderr-log-fn [min-level]
  (let [console-fn (trove-console/get-log-fn {:min-level min-level})]
    (fn [ns coords level id lazy_]
      (binding [*out* *err*]
        (console-fn ns coords level id lazy_)))))

(trove/set-log-fn! (make-stderr-log-fn :info))
```

### Custom log function

```clojure
(trove/set-log-fn!
  (fn [ns coords level id lazy_]
    (let [data (force lazy_)]
      ;; Send to your logging system
      (my-logger/log level id data))))
```

## Migrating from Timbre

Previous versions of Datahike used `taoensso/timbre` directly. If you had Timbre configuration
in your application (appenders, level settings, etc.), here is how to migrate.

### Keeping Timbre as your backend

If you want to keep using Timbre and all its appenders, add `com.taoensso/timbre` to your own
`deps.edn` and route Datahike's log output through it:

```clojure
(require '[taoensso.trove :as trove]
         '[taoensso.trove.timbre]) ;; ships with trove

(trove/set-log-fn! (taoensso.trove.timbre/get-log-fn))
```

All Datahike log events will now flow through your Timbre configuration — appenders, level
filters, and middleware all work as before. Your existing `timbre/merge-config!` calls continue
to apply.

### Replacing `timbre/set-level!`

Timbre's global level control is replaced by installing a log function with a `:min-level`:

```clojure
;; Before (Timbre)
(timbre/set-level! :warn)

;; After (trove console backend)
(require '[taoensso.trove :as trove]
         '[taoensso.trove.console :as trove-console])
(trove/set-log-fn! (trove-console/get-log-fn {:min-level :warn}))
```

### Replacing `timbre/merge-config!` with appenders

If you used custom Timbre appenders (e.g. file rotation, JSON output), you have two options:

1. **Keep Timbre** — use the `taoensso.trove.timbre` backend (see above) and keep your existing
   appender configuration unchanged.

2. **Drop Timbre** — write a custom log function that does what your appender did. For example,
   a simple file logger:

```clojure
(require '[taoensso.trove :as trove])
(import '[java.io FileWriter])

(let [writer (FileWriter. "/var/log/datahike.log" true)]
  (trove/set-log-fn!
    (fn [ns coords level id lazy_]
      (when (>= (.indexOf [:trace :debug :info :warn :error] level)
                (.indexOf [:trace :debug :info :warn :error] :info))
        (let [{:keys [msg data error]} (force lazy_)]
          (locking writer
            (.write writer (str (java.time.Instant/now) " " level " " id " " (or msg data) "\n"))
            (.flush writer)))))))
```

## Error Handling

Datahike uses the `log/raise` macro to log an error and throw an `ExceptionInfo` in one step.
The signature takes any number of message string fragments followed by a data map:

```clojure
(log/raise "Attribute " attr " is not defined in schema"
           {:attribute attr :error :schema/missing})
```

This logs at `:error` level (with source coordinates for CLJ-865 workaround) and throws:

```clojure
(ex-info "Attribute :foo is not defined in schema"
         {:attribute :foo :error :schema/missing})
```

The data map typically contains an `:error` or `:type` key describing the error category, plus
contextual keys like `:value`, `:attribute`, or `:binding`.

Contributors should use `log/raise` for all error paths instead of bare `throw`.
