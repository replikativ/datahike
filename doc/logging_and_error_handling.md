# Logging and Error Handling

**Note:** Datahike is transitioning from Timbre to trove/custom logging macros. As a library, Datahike should not force any logging implementation on users.

Historically, we chose Timbre for logging because it is the only library that comes with Clojure and ClojureScript
logging in one piece. Aside from that it offers easy appender implementation and is heavily used throughout
the Clojure community. You can use file logging and structured logging from built-in appenders.

However, as a library, Datahike should not force any logging implementation on users. We are transitioning to trove or custom logging macros that allow users to bring their own logging solution. Timbre can be configured via a simple Clojure map, or you can use your preferred Java logging implementation.

We needed a central place to raise an error and log at the same time in a consistent way. Therefore, we chose
to use a macro in the `tools` namespace named `raise` to solve that problem. In case you want to contribute to
Datahike please use this macro to throw errors. Please use logging appropriately in case you want to log in
another level.

## Logging Configuration

Logging of an error consists of the message as the first part and optionally of a map of details. These details mostly consist of an `:error` key that describes where to search for your error and some information regarding the input that created the error like `:value`, `:binding` or `:symbol`.

### Current Implementation (Transitional)

Currently, Datahike uses Timbre internally. Please see [Timbre on GitHub](https://github.com/ptaoussanis/timbre/) for details of the library. You can use it via any of your preferred logging implementations on the JVM as well as in JS.

There is a [bug in Clojure](https://clojure.atlassian.net/browse/CLJ-865) that prevents macros from passing line numbers to downstream macros. Thanks to Timbre we can use the low-level `log!` macro and pass it the line number. This is currently the only workaround to pass line numbers between macros.

An example to configure logging for an application using Datahike:

```clojure
(ns datahike-example-app.core
  (:require [taoensso.timbre :as log]
            [taoensso.timbre.appenders.3rd-party.rotor :as rotor]))

(log/merge-config! {:level :debug
                    :appenders {:rotating (rotor/rotor-appender
                                           {:path "/var/log/datahike-server.log"
                                            :max-size (* 512 1024)
                                            :backlog 10})}})
(log/infof "My first log in Datahike")
```

## Error Handling

Errors that are caught inside Datahike create an `ExceptionInfo` for Clojure or an `Error` for ClojureScript. It carries similar information to the logging of these errors. An error consists of the message as the first part and optionally a map of details. These details mostly consist of an `:error` key that describes where to search for your error and some information regarding the input that created the error like `:value`, `:binding` or `:symbol`.
