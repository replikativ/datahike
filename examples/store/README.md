# store example

We explore different kinds of persistence possibilities in datahike.

## Usage

Open `/src/example/core.clj` in your favourite editor, fire up a clojure repl
and eval the expressions from top to bottom. For the `postgres` example you need to have
[docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) installed.
Start it with:

``` sh
docker-compose up -d
```

If the selected ports collide with other ports, you may adjust `./docker-compose.yml`.
