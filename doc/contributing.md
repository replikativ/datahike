# Contributing to Datahike

The current process requires having [babashka](https://github.com/babashka/babashka) installed.

An overview of the available tasks will be shown with

```
bb tasks
```


## Compile Java classes
```
bb compile
```

## Start a REPL

```
bb rpl
```

Be careful not to type `bb repl` as this will start a babashka repl!

## Run the tests

Unittests

```
# Persistent sorted set index (fast)

bb test pss
#or 
bb test pss --watch

# Hitchhiker-tree index (slow)

bb test hht
```

Integration tests (Docker needed)

```
bb test integration
```

Backward-compatibility test

```
bb test back-compat
```

Native-image test (native-image needed)

```
bb test native-image
```

## Start the benchmarks
```
bb bench
```

## Build a Datahike jar
```
bb jar
```

## Install Datahike to local maven repository
```
bb install
```

## Format
Check the formatting:
```
bb format
```
or fix the formatting:
```
bb ffix
```

## Release Datahike
### Deploying Datahike to Clojars manually
#### Manually
**Should only be done in case of emergency**
First you have to build the artifact with `bb jar`, then you need to
set `CLOJARS_USERNAME` and `CLOJARS_PASSWORD` as environment variables, then
you can run `bb deploy` to deploy the artifact to Clojars.

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
Each merge to `main` creates a draft release on GitHub and a git tag to point to the merge commit
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
