# Distribution

Datahike supports two types of distributed access, *distribution of data* or
*distribution of computation*. Distribution of data means that you can access
data sources in what we call a distributed index space (DIS), while distribution
of computation means that you send requests for things to be evaluated to
another machine.

# Distributed index space (DIS)

Datahike has a similar memory model to [Datomic](https://datomic.com) , which is
build on distributed persistent indices. But while Datomic requires active
connections to the transactor, Datahike works with lightweight connections that
do not require communication by default.

In case where you do not need to write to a database, only *read* from it, e.g.
a database that a 3rd party provides you access to, it is sufficient to have
read access rights to the store, no setup of a server or additional steps are
needed to join against the indices of this external database!

Note: This allows you to *massively shard* databases. A good design pattern is
to create a separate database for a set of facts that you need to consistently
update together, e.g. one database per business client.

## Single writer

If you want to provide distributed write access to databases you need to setup
a server as described in the section at the end. Datahike then centralizes all
write operations and state changes to the database on this single machine, while
all read operations still can happen in locally on as many machines as have
access to the store. The benefit of the single writer is that it provides strong
linearization guarantees for transactions, i.e. strong consistency.

The client setup is simple, you just add a `:writer` entry in the configuration
for your database, e.g.

```clojure
{:store  {:backend :mem :id "distributed-datahike"}
 :keep-history?      true
 :schema-flexibility :read
 :writer             {:backend :datahike-server
                      :url     "http://localhost:4444"
                      :token   "securerandompassword"}}
```

You can now use the normal `datahike.api` as usual and all operations changing a
database, e.g. `create-database`, `delete-database` and `transact` are sent to
the server while all other calls are executed locally.

### AWS lambda

An example setup to run Datahike distributed in AWS lambda without a server can
be found [here](https://github.com/viesti/clj-lambda-datahike). It configures a
singleton lambda for write operations while reader lambdas be run multiple times
and scale out.

# Distribution of compute

Datahike supports sending all requests to a server. This has the benefit that
the server will do all the computation and its caches will be shared between
different clients. The disadvantage is that you cannot easily share information
in process, e.g. call your own functions or closures in queries without
deploying them to the server first.

## Remote procedure calls (RPCs)

The remote API has the same call signatures as `datahike.api` and is located in
`datahike.api.client`. Except for listening and `with` all functionality is
supported. Given a server is setup (see below), you can interact with by adding
`:remote-peer` to the config you would otherwise with `datahike.api`:

```clojure
{:store  {:backend :mem :id "distributed-datahike"}
 :keep-history?      true
 :schema-flexibility :read
 :remote-peer        {:backend :datahike-server
                      :url     "http://localhost:4444"
                      :token   "securerandompassword"}}
```

The API will return lightweight remote pointers that follow the same semantics
as `datahike.api`, but do not support any of Datahike's local functionality,
i.e. you can only use them with this API.

# Combined distribution

Note that you can combine both data accesses, i.e. run a set of servers sharing
a single writer among themselves, while they all serve a large set of outside
clients through RPCs.

## Setup datahike.http.server

To build it locally you only need to clone the repository and run `bb
http-server-uber` to create the jar. The server can then be run with `java -jar
datahike-http-server-VERSION.jar path/to/config.edn`.

The edn configuration file looks like:

```clojure
{:port     4444
 :level    :debug
 :dev-mode true
 :token    "securerandompassword"}
```

Port sets the `port` to run the HTTP server under, `level` sets the log-level.
`dev-mode` deactivates authentication during development and if `token` is
provided then you need to send this token as the HTTP header "token" to
authenticate.

The server exports a swagger interface on the port and can serialize requests in
`transit-json` and `edn` (JSON support is planned). The server exposes all
referentially transparent calls (that don't change given their arguments) as GET
requests and all requests that depend on input information as POST requests. All
arguments in both cases are sent as a list *in the request body*.

### Extended configuration

CORS headers can be set, e.g. with adding
```clojure
 :access-control-allow-origin [#"http://localhost" #"http://localhost:8080"]
```

The server also experimentally supports HTTP caching for GET requests, e.g. by adding
```clojure
 :cache {:get {:max-age 3600}}
```

This should be beneficially in case your HTTP client or proxy supports efficient
caching and you often run the same queries many times on different queries (e.g.
to retrieve a daily context in an app.)
