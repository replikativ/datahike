#!/usr/bin/env bb
;; Generate TypeScript definitions for npm package

(require '[datahike.js.typescript :as ts])

(println "Generating TypeScript definitions...")
(ts/write-type-definitions! "npm-package/index.d.ts")
(println "TypeScript definitions generated at npm-package/index.d.ts")
