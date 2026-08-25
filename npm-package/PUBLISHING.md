# npm publication

Datahike publishes the same generated version to Clojars and npm. The release
workflow builds and tests the npm tarball before either deployment, deploys the
JAR, publishes the already verified tarball, and finally creates the GitHub
release.

Normal releases do not use a long-lived `NPM_TOKEN`. CircleCI exchanges its
job identity for a short-lived npm token using trusted publishing (OIDC).

## One-time npm and CircleCI setup

The person configuring this needs maintainer access to the `datahike` package,
admin access to the CircleCI project, and an npm CLI version that supports
trusted-publisher management.

1. In CircleCI, open **Organization Settings > Contexts** and create an empty
   context named `npm-publish`. Do not add an npm token.
2. Restrict the context to the Datahike project. Add this expression restriction:

   ```text
   pipeline.git.branch == "main" and not job.ssh.enabled and not (pipeline.config_source starts-with "api")
   ```

3. Record these CircleCI UUIDs:

   - organization ID
   - Datahike project ID
   - pipeline definition ID
   - the `npm-publish` context ID

4. On npmjs.com, open **datahike > Settings > Trusted Publisher**, choose
   **CircleCI**, and enter:

   - organization, project, and pipeline definition IDs from step 3
   - VCS origin: `github.com/replikativ/datahike`
   - context ID: the `npm-publish` context
   - allowed action: `npm publish`

   The equivalent CLI command is:

   ```bash
   npm trust circleci datahike \
     --org-id <organization-uuid> \
     --project-id <project-uuid> \
     --pipeline-definition-id <pipeline-definition-uuid> \
     --vcs-origin github.com/replikativ/datahike \
     --context-id <context-uuid> \
     --allow-publish
   ```

5. Leave the workflow's `npm-publish` context attachment in
   `.circleci/config.yml`. CircleCI supplies the identity claims; the job
   requests the npm audience and publishes with `NPM_ID_TOKEN`.

npm allows one trusted publisher per package. CircleCI trusted publishing
requires Node 22.14 or newer and npm 11.5.1 or newer; the job pins those
minimums. CircleCI's provider currently does not support npm provenance
attestations, so the job intentionally does not pass `--provenance`.

## Release behavior

On `main`, after all tests pass:

1. CircleCI creates a single npm tarball with `bb npm-pack`.
2. The deploy job publishes the matching JAR to Clojars.
3. The `npm-publish` job checks whether that exact version already exists.
4. If absent, it obtains an OIDC token and publishes the tarball.
5. The GitHub release job runs only after both registries succeed.

The existence check makes a rerun safe after npm has accepted the immutable
version. It does not rebuild or silently replace an npm artifact.

## Local verification

```bash
bb npm-build
bb npm-test
bb npm-pack
npm pack --dry-run ./npm-package
```

`bb npm-build` synchronizes `npm-package/package.json` with the version
derived from `config.edn`, regenerates `index.d.ts`, and builds the Node,
browser, and optional S3 entry points with advanced optimizations.

For an installation smoke test, install the generated
`datahike-<version>.tgz` into a temporary project. Manual publication should
be reserved for release recovery and requires explicit maintainer
authentication; the regular path is CircleCI.

## Troubleshooting

- **OIDC authentication fails:** verify all four UUIDs, the VCS origin, and that
  the job has the `npm-publish` context.
- **Context is unauthorized:** check the project restriction and expression;
  SSH reruns and API-supplied configurations are deliberately denied.
- **Version already exists:** npm versions are immutable. Confirm the published
  tarball, then rerun the workflow; the job will skip that version.
- **`npm whoami` fails:** it is not an OIDC validation step. Validate by
  publishing through the protected release workflow.
- **Types or entry points are missing:** run `bb npm-build` and
  `npm pack --dry-run ./npm-package`, and inspect the listed files.

References:

- [npm trusted publishers](https://docs.npmjs.com/trusted-publishers/)
- [CircleCI OIDC tokens with custom claims](https://circleci.com/docs/guides/permissions-authentication/oidc-tokens-with-custom-claims/)
- [CircleCI contexts](https://circleci.com/docs/guides/security/contexts/)
