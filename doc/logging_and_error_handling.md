# Logging and Error Handling

We chose to use Timbre for logging because it is the only library that comes with clojure and clojurescript
logging in one piece. Aside from that it offers to implement appenders easily and seems heavy used throughout
the clojure community. You can use file logging and structured logging from built-in appenders.

As a library datahike should not force any logging implementation on the user so Timbre does offer a wide
variety of options. It can do it itself with a simple edn configuration or you can use your preferred Java
implementation.

We needed a central place to raise an error and log at the same time in a consistent way. Therefore we chose
to use a macro in the tools namespace named `raise` to solve that problem. In case you want to contribute to
Datahike please use this macro to throw errors. Please use logging appropriately in case you want to log in
another level.

## Logging via the timbre logging library

Please see [Timbre on GitHub](https://github.com/ptaoussanis/timbre/) for details of the library. You can
use it via any of your preferred logging implementations on the JVM as well as in JS. Logging of an error
consists of the message as the first part and optionally of a map of details. These details mostly consist
of an `:error` key that describes where to search for your error and some information regarding the input
that created the error like `:value`, `:binding` or `:symbol`.

There is a [bug in Clojure](https://clojure.atlassian.net/browse/CLJ-865) that prevents macros to pass
line numbers to downstream macros. Thanks to Timbre we can use the low-level `log!` macro and pass it the
line number. This seems the only workaround at the time to pass line numbers between macros.

An example to configure logging for an application using Datahike:

```
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

Errors that are caught inside datahike create an `Execution error` that carries similar information like the
logging of these errors. An error consists of the message as the first part and optionally of a map of
details. These details mostly consist of an `:error` key that describes where to search for your error
and some information regarding the input that created the error like `:value`, `:binding` or `:symbol`.
