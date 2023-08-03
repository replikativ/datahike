# Datahike Babashka Pod

This doc is about using Datahike from a [babashka pod](https://babashka.org/).

## Why Datahike as a babashka pod?

A babashka pod is a way to use Datahike in babashka without Datahike being built into the babashka binary. When you
don't want to run a whole JVM for a little script you can use babashka and write your script in your preferred language.
Babashka already has ways to persist data to other databases but Datahike adds the possibility to write to a durable
datalog database.
