# Examples

This project shows some use cases for the store, the schema, and temporal index implemented in datahike.

## Usage

Open in `/src/examples` the topic you want to explore in your favourite editor, use a clojure repl to
eval the expressions from top to bottom. 

For the [PostgreSQL](https://www.postgresql.org) example in `store.clj` you need to have 
[docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) installed.
Start it with:

``` sh
docker-compose up -d
```

If the selected ports collide with other ports, you may want to adjust `/docker-compose.yml.` and restart the container.
