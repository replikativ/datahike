var datahike = {};
datahike.db = {};

/**
 * @constructor
 */
datahike.db.Datom = function() {};
datahike.db.Datom.prototype.e;
datahike.db.Datom.prototype.a;
datahike.db.Datom.prototype.v;
datahike.db.Datom.prototype.tx;


datahike.impl = {};
datahike.impl.entity = {};

/**
 * @constructor
 */
datahike.impl.entity.Entity = function() {};
datahike.impl.entity.Entity.prototype.db;
datahike.impl.entity.Entity.prototype.eid;
datahike.impl.entity.Entity.prototype.keys      = function() {};
datahike.impl.entity.Entity.prototype.entries   = function() {};
datahike.impl.entity.Entity.prototype.values    = function() {};
datahike.impl.entity.Entity.prototype.has       = function() {};
datahike.impl.entity.Entity.prototype.get       = function() {};
datahike.impl.entity.Entity.prototype.forEach   = function() {};
datahike.impl.entity.Entity.prototype.key_set   = function() {};
datahike.impl.entity.Entity.prototype.entry_set = function() {};
datahike.impl.entity.Entity.prototype.value_set = function() {};
