# NPM Publication Guide

This guide explains how to publish Datahike to npm.

## Prerequisites

1. npm account (create at https://www.npmjs.com/signup)
2. Login to npm: `npm login`

## Files Included in Package

The `package.json` specifies which files to include:
- All `.js` files (compiled ClojureScript)
- All `.js.map` files (source maps)
- `index.d.ts` (TypeScript definitions)
- `README.md` (documentation)
- `LICENSE` (EPL-1.0)

## Pre-Publication Checklist

1. **Build the npm package** (this handles version, types, and compilation)
   ```bash
   bb npm-build
   ```
   
   This single command will:
   - Update `package.json` version from `config.edn` (major.minor.commit-count)
   - Regenerate TypeScript definitions
   - Compile ClojureScript with shadow-cljs
   
   Alternatively, run individual steps:
   ```bash
   bb npm-version        # Update package.json version only
   bb codegen-ts         # Generate TypeScript definitions only
   npx shadow-cljs compile npm-release  # Compile ClojureScript only
   ```

2. **Run tests**
   ```bash
   cd npm-package
   node test.js
   ```

3. **Verify package contents**
   ```bash
   cd npm-package
   npm pack --dry-run
   ```

## Publishing

### Publish to npm

```bash
cd npm-package
npm publish
```

### Tag a release

After publishing, tag the git commit:
```bash
git tag v0.7.0-alpha1
git push --tags
```

## Post-Publication

1. Verify the package on npm: https://www.npmjs.com/package/datahike
2. Test installation: `npm install datahike@latest`
3. Create GitHub release with changelog

## Testing the Package Locally

Before publishing, test the package locally:

```bash
# In npm-package directory
npm pack

# This creates datahike-0.7.0-alpha1.tgz
# In another project:
npm install /path/to/datahike/npm-package/datahike-0.7.0-alpha1.tgz
```

## Version Strategy

Datahike uses a **centralized version management** system:
- **Source of truth**: `config.edn` contains `{:version {:major X :minor Y}}`
- **Full version**: Automatically calculated as `major.minor.commit-count` (e.g., `0.6.1634`)
- **Synchronized**: Same version used for JVM (Maven/Clojars), ClojureScript, and npm

To increment the version:
```bash
bb inc major   # Increment major version (breaking changes)
bb inc minor   # Increment minor version (new features)
```

The `bb npm-build` command automatically syncs the current version to `package.json`.

## Troubleshooting

### "Module not found" errors
Check that `package.json` `main` field points to the correct entry file:
```json
"main": "datahike.js.api.js"
```

### TypeScript types not recognized
Ensure `types` field in `package.json` is correct:
```json
"types": "index.d.ts"
```

### Missing dependencies
Datahike is compiled to standalone JavaScript - no runtime dependencies needed.

## Updating the Package

1. Make code changes in ClojureScript source
2. Build package: `bb npm-build` (handles compilation, types, and version)
3. Update CHANGELOG.md if needed
4. Run tests: `node test.js`
5. Publish: `npm publish`

## Quick Release Checklist

1. Ensure all changes are committed
2. Increment version if needed: `bb inc [major|minor]`
3. Build package: `bb npm-build`
4. Test: `cd npm-package && node test.js`
5. Verify: `npm pack --dry-run`
6. Login: `npm login`
7. Publish: `npm publish`

## Links

- npm package: https://www.npmjs.com/package/datahike
- npm docs: https://docs.npmjs.com/
- TypeScript declarations: https://www.typescriptlang.org/docs/handbook/declaration-files/introduction.html
