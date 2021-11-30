# Contributing to Datahike
## Starting a REPL

```
clj -M:repl
```

## Running the tests
- `./bin/run-unittests` or `./bin/run-unittests --watch`
- `./bin/run-integrationtests` (Docker needed)

## Starting the benchmarks
```
TIMBRE_LEVEL=':info' clj -M:benchmark run
```

## Building a Datahike jar
```
clj -T:build jar
```

## Install Datahike to local maven repository
```
clj -T:build install
```

## Formatting
Check the formatting:
```
clj -M:format
```
or fix the formatting:
```
clj -M:ffix
```

## Releasing Datahike
### Deploying Datahike to Clojars manually
#### Manually
**Should only be done in case of emergency**
First you have to build the artifact with `clj -T:build jar`, then you need to
set `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` as environment variables, then
you can run `clj -T:build deploy` to deploy the artifact to Clojars.

#### Pipeline
[Datahike is released everytime there is a commit to the `main` branch](https://github.com/replikativ/datahike/blob/development/doc/adl/adr-004-github-flow.md).
We are using semantic versioning and the patch-version is the number of commits on
the main branch. That means it is always increasing, even if the major or minor
version is increased.

### CircleCI
Clojars needs to be stored as an environment variable on CircleCI. There needs to be a
variable `CLOJARS_USERNAME` set to your Clojars username and a variable `CLOJARS_PASSWORD` set
to the token that permits to deploy on clojars.

In order to create new releases on GitHub we need environment variables on CircleCI. The
two variables `GITHUB_TOKEN` and `GITHUB_USER` need to be set in a context called
`github-token` in the CircleCI UI for the organisation.

### Git tags and GitHub releases
Each merge to `main` creates a release entry in GitHub and a git tag to point to the merge commit
made when merging a branch into `main`. The jar is appended to the Github-release.

### The release process step by step
- Update the `[CHANGELOG.md](https://github.com/replikativ/datahike/blob/main/CHANGELOG.md)`
- Set a  new version in build.clj if you want to release a new minor or major version.
  For the ordinary patch release you can let the CI automatically increment the patch
  release version.
- Squash and rebase if you see a need to.
- Create the PR against the `main` branch.
- Once approved, merge the PR into `main`.
  + a new release on Clojars will be created.
  + a new draft release will be created on GitHub.
- Go to [GitHub](https://github.com/replikativ/datahike/releases) and
  + fill in release notes
  + release the draft Release.
