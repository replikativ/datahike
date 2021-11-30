# Github Flow

## Context

[GitHub Flow](https://githubflow.github.io/)

We want to release more often. Therefore we are considering the GitHub Flow as an alternative to
our current Gitflow. Everytime a merge-request gets merged a new release should be created after
running the pipeline. That means features and bugfixes are propagated faster. Apart from that
the flow is just simpler and sufficient for our and our users' needs.

[Discussion](https://github.com/replikativ/datahike/discussions/447)

## Options

[4 branching workflows for Git](https://medium.com/@patrickporto/4-branching-workflows-for-git-30d0aaee7bf)

## Status

**Proposed**

## Decision

We are switching to the GitHub Flow with [PR #445](https://github.com/replikativ/datahike/pull/445).

## Consequences

When the PR is merged into the newly created `main`-branch we are from then on only branching off
of main for feature- and bugfix-branches. `main` needs to be the new default branch where everyone
is landing on by default. Everytime a PR is merged there will be a new release on GitHub with the
patch-level set to the overall commit-count. To make a new minor or major release the number
needs to be adjusted manually in the build.clj-file.

This change needs to be announced properly so that everyone working on Datahike knows about this
new way of working. In the future all replikativ-libraries will be switched so everyone needs to
know that there will not be any long-living-branches any more.

Another consequence to consider is that CircleCI then has permission to write to our GitHub-repo.
Since it is necessary to write to the git-repo when tagging a commit CircleCI needs the full
write access. It already has the ability to release a jar to Clojars.
