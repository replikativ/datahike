import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

const compiled = "target/ts-client";
const output = "npm-package/remote";
fs.mkdirSync(output, { recursive: true });

const core = fs.readFileSync(path.join(compiled, "core.js"), "utf8")
  .replace(/^export /gm, "");
const generated = fs.readFileSync(path.join(compiled, "api.generated.js"), "utf8")
  .replace(/^import .*;\n/gm, "");
const names = [...generated.matchAll(/^export function ([A-Za-z0-9_$]+)/gm)]
  .map((match) => match[1])
  .filter((name, index, all) => all.indexOf(name) === index);
const helpers = ["clearCache", "configureCache", "isPromise", "listen", "randomUuid", "unlisten", "uuid"];
const esm = `${core}\n${generated}\nexport { ${helpers.join(", ")} };\n` +
  `export default { ${[...names, ...helpers].join(", ")} };\n`;

fs.writeFileSync(path.join(output, "index.mjs"), esm);
fs.writeFileSync(
  path.join(output, "index.js"),
  ts.transpileModule(esm, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText + "\nmodule.exports = exports.default;\n",
);
for (const file of ["api.generated.d.ts", "core.d.ts", "index.d.ts"]) {
  fs.copyFileSync(path.join(compiled, file), path.join(output, file));
}
