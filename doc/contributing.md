# Contributing to Datahike
## Starting a REPL

```
clj -M:repl
```

## Running the tests
- `./bin/run-unittests` or `./bin/run-unittests --watch`
- `./bin/run-integrationtests` (Docker needed)

## Starting the benchmark
```
TIMBRE_LEVEL=':info' clj -M:benchmark
```

## Building a Datahike jar
```
mvn compile
clj -M:build
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
### Deploying Datahike to Clojars
```
clj -M:deploy
```

### SNAPSHOT
To create a snapshot of Datahike and upload it to Clojars the version in
`pom.xml` needs to end on *-SNAPSHOT*. Snapshots can and will be overwritten
if a snapshot with the current version is already present. There is no easy way
to recognize that there is a new snapshot available on Clojars.

A snapshot in Datahike is created by committing/merging on and pushing to the
*development* branch. Then the snapshot gets validated. This means the version
string from `pom.xml` is extracted and it needs to end on *-SNAPSHOT* and
the current branch needs to be *development*. Only then a snapshot gets released
to Clojars.

### Release
To create a release of Datahike and upload it to Clojars the version in `pom.xml`
needs to be incremented at the respective position. Then you can commit and push the
current version to GitHub. CircleCI will then unittest, integrationtest and build
this version of Datahike. When this succeeds it releases the new version to Clojars
only if the version string does *not* end on *-SNAPSHOT* and the current branch is
*master*.

The deploy to Clojars will fail if this version is already present on Clojars. It is
not possible to overwrite a release, only snapshots can be overwritten.

### CircleCI
On CircleCI there will be run unittests, integrationtests, Datahike will be compiled and
released. The official CircleCI jdk-8 image is used and the secret key to release to
Clojars needs to be stored as an environment variable on CircleCI. There needs to be a
variable `CLOJARS_USERNAME` set to your Clojars username and a variable `CLOJARS_PASSWORD` set
to the token that permits to deploy on clojars.

### Git tags
It is nice to have git tags for releases, especially for non-snapshot releases. Therefor
you can use following small hook that must be copied into the folder `.git/hooks/` as
`post-commit` and a duplicate as `post-merge`. It creates annotated tags when committing
on or merging into master.

Snapshots should not be tagged because it might be necessary to publish multiple
snapshots and then you would have to force-push them.

```bash
#!/bin/bash

BRANCH=$(git rev-parse --abbrev-ref HEAD)
REMOTE=$(git config --get remote.origin.url)
TAG=$(xmllint --xpath '/*[local-name()="project"]/*[local-name()="version"]/text()' pom.xml)

# tag current release
if [[ ${BRANCH} == master ]] \
    && [[ ${REMOTE} =~ "replikativ/datahike.git" ]] \
    && [[ ! ${TAG} =~ "-SNAPSHOT" ]]; then
    echo "tagging release ${TAG}"
    git tag -a -m "${TAG}" ${TAG}
fi
```

For this to work you need to set following option at the [appropriate level](https://www.git-scm.com/book/en/v2/Customizing-Git-Git-Configuration#_git_config):
```
git config push.followTags true
```
Only then the tag will be pushed without a second push with the `--tags` option.

### The release process step by step
- Make sure the versions of dependencies declared in deps.edn and pom.xml match.
- Set the new version in pom.xml.
  + If you just want to create a snapshot version add '-SNAPSHOT' to the version, e.g <version>0.3.6-SNAPSHOT</version>.
- Create the PR and ask for it to be merged to the development branch.
- Once approved, merge the PR to dev.
  + This will create a SNAPSHOT version on clojars, if the version in pom contains '-SNAPSHOT'.
- Repeat the above process for the other PRs to be released.
- Optional: ideally try to import the new jar into a project and test that it works.

- Create a new branch for the release called e.g. 'Release v0.3.6'
  + Set the new version number in pom.xml, i.e., <version>0.3.6</version> (without '-SNAPSHOT' this time).
  + Update the CHANGELOG.md file.
- Create a PR with the new release branch with 'master' as target.
- Once approved, merge the PR to master.
  + This will deploy the new release to clojars.
- Test the new jar (e.g. using in a real project).
- Create a new release/tag on github https://github.com/replikativ/datahike/releases
