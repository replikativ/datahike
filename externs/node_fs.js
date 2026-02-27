/**
 * @externs
 * Additional Node.js module externs not covered by shadow-cljs defaults.
 *
 * Needed because cljs-node-io and konserve.node-filestore use fs/stream APIs
 * that get renamed by Closure Compiler advanced optimization without these externs.
 */

// ---------------------------------------------------------------------------
// fs module — sync methods used by environ.core and konserve.node-filestore
// ---------------------------------------------------------------------------
var fs = {};
fs.existsSync = function(path) {};
fs.readFileSync = function(path, options) {};
fs.writeFileSync = function(path, data, options) {};
fs.fstatSync = function(fd) {};
fs.statSync = function(path) {};
fs.openSync = function(path, flags, mode) {};
fs.closeSync = function(fd) {};
fs.readSync = function(fd, buffer, offset, length, position) {};
fs.writeSync = function(fd, buffer, offset, length, position) {};
fs.fsyncSync = function(fd) {};
fs.mkdirSync = function(path, options) {};
fs.readdirSync = function(path, options) {};
fs.unlinkSync = function(path) {};
fs.renameSync = function(oldPath, newPath) {};
fs.copyFileSync = function(src, dest, flags) {};
fs.createReadStream = function(path, options) {};
fs.createWriteStream = function(path, options) {};

// fs.Stats object returned by fstatSync/statSync
var Stats = {};
Stats.prototype.size;
Stats.prototype.isFile = function() {};
Stats.prototype.isDirectory = function() {};

// ---------------------------------------------------------------------------
// stream module — class names used by cljs-node-io protocol extensions.
// Without these, Closure renames stream.Writable → stream.zr etc. and
// the resulting prototype extensions fail at runtime.
// ---------------------------------------------------------------------------
var stream = {};
/** @constructor */
stream.Readable = function() {};
/** @constructor */
stream.Writable = function() {};
/** @constructor */
stream.Duplex = function() {};
/** @constructor */
stream.Transform = function() {};
/** @constructor */
stream.PassThrough = function() {};
