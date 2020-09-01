# How to do a release for Datahike
## SNAPSHOT
To create a snapshot of Datahike and upload it to Clojars the version in
`project.clj` needs to end on *-SNAPSHOT*. Snapshots can and will be overwritten
if a snapshot with the current version is already present. There is no easy way
to recognize that there is a new snapshot available on Clojars.

A snapshot in Datahike is created by committing/merging on and pushing to the
*development* branch. Then the snapshot gets validated. This means the version
string from `project.clj` is extracted and it needs to end on *-SNAPSHOT* and
the current branch needs to be *development*. Only then a snapshot gets released
to Clojars.

## Release
To create a release of Datahike and upload it to Clojars the version in `project.clj`
needs to be incremented at the respective position. Then you can commit and push the
current version to GitHub. CircleCI will then unittest, integrationtest and build
this version of Datahike. When this succeeds it releases the new version to Clojars
only if the version string does *not* end on *-SNAPSHOT* and the current branch is
*master*.

The deploy to Clojars will fail if this version is already present on Clojars. It is
not possible to overwrite a release, only snapshots can be overwritten.

## CircleCI
On CircleCI there will be run unittests, integrationtests, Datahike will be compiled and
released. The official CircleCI jdk-8 image is used and the secret key to release to
Clojars needs to be stored as an environment variable on CircleCI. There needs to be a
variable `LEIN_USERNAME` set to your Clojars username and a variable `LEIN_PASSWORD` set
to the password that permits to deploy on clojars.

## Git tags
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
TAG=$(head -n 1 project.clj | awk '{print $3}' | tr -d \")

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
