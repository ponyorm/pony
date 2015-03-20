(function (pony, undefined) {

    pony.entities = {};
    pony.objects = {};
    pony.nextId = 1;
    pony.nextNewId = 1;
    pony.modifiedObjects = [];
    pony.cacheModified = ko.observable(false);

    pony.converters = {
        int: {
            validate: function(val, attr, obj) {
                var result = +val;
                if (!isNaN(result)) {
                    obj.clearErrorMsg(attr);
                    return result;
                }
                obj.setErrorMsg(attr, 'Should be integer');
                return val;
            }
        }
    };

    var assert = function (expr, msg) {
        if (!expr) {
            throw new Error(msg || 'Assertion failed!');
        }
    };

    var Attribute = function (entity, name, kind, type) {
        var attr = this;
        attr.entity = entity;
        attr.name = name;
        attr.kind = kind;
        attr.type = type;
        attr.isCollection = (kind === 'Set');
        entity.attrs[name] = attr;
    };
    Attribute.prototype.toString = function () {
        return this.entity.name + '.' + this.name;
    };
    Attribute.prototype.get = function (obj) {
        var attr = this;
        var val = obj._vals_[attr.name];
        if (attr.isCollection && val !== undefined) {
            val = val.slice(0);
        }
        return val;
    };
    Attribute.prototype.set = function (obj, val, fromDb, isReverseCall) {
        var attr = this;
        val = attr.validate(val, obj, fromDb);
        var attrName = attr.name;
        var prev = fromDb ? obj._dbvals_[attrName] : obj._vals_[attrName];
        if (attr.isCollection) {
            var added = _.difference(val, prev);
            var removed = fromDb ? [] : _.difference(prev, val);
            if (!added.length && !removed.length) {
                return;
            }
        }
        else if (prev === val) {
            return;
        }
        if (!fromDb) {
            obj._vals_[attrName] = val;
            obj._newvals_[attrName] = val;
        } else if (!attr.isCollection) {
            obj._dbvals_[attrName] = val;
            if (obj._newvals_[attrName] === undefined) {
                obj._vals_[attrName] = val;
            }
        } else {
            obj._dbvals_[attrName] = prev.concat(added);
            // obj._vals_[attrName] = _.union(obj._vals_[attrName], added)
            var vals = obj._vals_[attrName]; // optimization
            var diff = _.difference(added, vals);
            obj._vals_[attrName] = vals.concat(diff);
        }
        var innerObservable = obj._observables_[attrName];
        innerObservable(attr.get(obj));
        if (!fromDb) {
            obj._setModified_();
        }
        var reverse = attr.reverse;
        if (!reverse) {
            return;
        }
        if (!attr.isCollection) {
            if (!reverse.isCollection) {
                // one-to-one
                if (prev !== null && prev !== undefined) {
                    if (!isReverseCall || val !== null) {
                        reverse.set(prev, null, fromDb, true);
                    }
                }
                if (val !== null && !isReverseCall) {
                    reverse.set(val, obj, fromDb, true);
                }
            } else {
                // many-to-one
                if (prev !== null && prev !== undefined) {
                    reverse.remove(prev, obj, fromDb, true);
                }
                if (val !== null && !isReverseCall) {
                    reverse.add(val, obj, fromDb, true);
                }
            }
        } else {
            if (!reverse.isCollection) {
                // one-to-many
                _.each(removed, function (item) {
                    reverse.set(item, null, fromDb, true);
                });
                _.each(added, function (item) {
                    reverse.set(item, obj, fromDb, true);
                })
            } else {
                // many-to-many
                _.each(removed, function (item) {
                    reverse.remove(item, obj, fromDb, true);
                });
                _.each(added, function (item) {
                    reverse.add(item, obj, fromDb, true);
                })
            }
        }
    };
    Attribute.prototype.create = function (obj, vals) {
        var attr = this, reverse = attr.reverse, reverseAttrName = reverse.name;
        if (vals === undefined) {
            vals = {};
        }
        else if (vals[reverseAttrName] !== undefined) {
            attr.error('You should not pass value of ' + reverseAttrName + ' attribute to ' + String(attr)
                       + '.create() options because it will be set automatically to ' + String(obj));
        }
        var item = attr.type.create(vals);
        attr.add(obj, item);
        return item;
    };
    Attribute.prototype.add = function (obj, item, fromDb, isReverseCall) {
        var attr = this;
        item = attr.validate([item], obj)[0];
        var attrName = attr.name;
        var val = obj._vals_[attrName];
        if (fromDb) {
            var dbval = obj._dbvals_[attrName];
            if (!_.contains(dbval, item)) {
                dbval.push(item);
            }
        }
        if (!_.contains(val, item)) {
            val.push(item);
            if (!fromDb) {
                obj._newvals_[attrName] = val;
            }
            var innerObservable = obj._observables_[attrName];
            innerObservable(attr.get(obj));
            if (!fromDb) {
                obj._setModified_();
            }
        }
        if (!isReverseCall) {
            var reverse = attr.reverse;
            if (!reverse.isCollection) {
                reverse.set(item, obj, fromDb, true);
            } else {
                reverse.add(item, obj, fromDb, true);
            }
        }
    };
    Attribute.prototype.remove = function (obj, item, fromDb, isReverseCall) {
        var attr = this;
        assert(!fromDb);
        item = attr.validate([item], obj)[0];
        var attrName = attr.name;
        var val = obj._vals_[attrName];
        var i = _.indexOf(val, item);
        if (i >= 0) {
            val.splice(i, 1);
            obj._newvals_[attrName] = val;
            var innerObservable = obj._observables_[attrName];
            innerObservable(attr.get(obj));
            obj._setModified_();
        }
        if (!isReverseCall) {
            var reverse = attr.reverse;
            if (!reverse.isCollection) {
                reverse.set(item, null, fromDb, true);
            } else {
                reverse.remove(item, obj, fromDb, true);
            }
        }
    };
    Attribute.prototype.validate = function (val, obj, fromDb) {
        var attr = this;
        if (val === null) {
            return val;
        }
        if (val === undefined) {
            attr.error('Attribute $attr cannot be set to undefined')
        }
        if (attr.isCollection) {
            if (!(val instanceof Array)) {
                attr.error('Value of $attr should be array of $type. Got: $val', val);
            }
            val = _.uniq(val);
            val = _.map(val, function (item) {
                if (!(item instanceof Instance)) {
                    return attr.getItemByPk(item, fromDb)
                }
                if (item._entity_.root !== attr.type.root) {
                    attr.error('$attr item should be $type instance. Got: $val', val);
                }
                return item;
            });
        } else if (val instanceof Array) {
            attr.error('Value of $attr should not be array');
        }
        else if (attr.reverse) {
            if (!(val instanceof Instance)) {
                val = attr.getItemByPk(val, fromDb);
            }
            if (val._entity_.root !== attr.type.root) {
                attr.error('Value of $attr should be of $type instance. Got: $val', val);
            }

        } else if (val instanceof Instance) {
            attr.error('Value of $attr should be $type. Got: $val', val);
        }
        var converter = pony.converters[attr.type];
        if (converter !== undefined) {
            val = converter.validate(val, attr, obj);
        }
        return val;
    };
    Attribute.prototype.getItemByPk = function (pk, fromDb) {
        var attr = this;
        return attr.type.getByPk(pk, fromDb);
    };
    Attribute.prototype.error = function (errmsg, val) {
        var attr = this;
        errmsg = errmsg.split("$attr").join(String(attr));
        errmsg = errmsg.split("$type").join(attr.reverse ? attr.type.name : attr.type);
        errmsg = errmsg.split("$val").join(val instanceof Instance ? String(val) : typeof val + ' ' + String(val));
        throw new Error(errmsg);
    };
    pony.Attribute = Attribute;

    var Entity = function (entityJson) {
        var entity = this;
        entity.name = entityJson.name;
        entity.attrs = {};
        var bases = entityJson.bases ? entityJson.bases.map(function (name) {
            return pony.entities[name]
        }) : [];
        entity.bases = bases;
        if (bases.length) {
            var root = bases[0];
            entity.root = root;
            entity.identityMap = root.identityMap;
        } else {
            entity.root = entity;
            entity.identityMap = {};
        }
        entity.root = bases.length ? bases[0] : entity;
        _.each(bases, function (base) {
            _.each(base.attrs, function (attr, name) {
                entity.attrs[name] = attr;
            });
        });
        _.each(entityJson.newAttrs, function (attrJson, j) {
            var attr = new Attribute(entity, attrJson.name, attrJson.kind, attrJson.type);
            if (attrJson.auto) {
                attr.auto = true;
            }
            if (attrJson.reverse) {
                attr.reverse = attrJson.reverse;
            }
            if (attrJson.nullable) {
                attr.nullable = true;
            }
        });
        entity.pkAttrs = _.map(entityJson.pkAttrs, function (attrName) {
            return entity.attrs[attrName];
        });
        entity.compositePk = entity.pkAttrs.length > 1;
        entity.autoPk = entity.pkAttrs.length === 1 && entity.pkAttrs[0].auto;
        pony.entities[entity.name] = entity;
    };
    Entity.prototype.toString = function () {
        return 'Entity:' + this.name;
    };
    Entity.prototype.checkAttrName = function (attrName) {
        var entity = this;
        if (entity.attrs[attrName] === undefined) {
            throw new Error('Entity ' + entity.name + ' does not have attribute ' + attrName);
        }
    };
    Entity.prototype.linkReverseAttrs = function () {
        var entity = this;
        _.each(entity.attrs, function (attr) {
            if (attr.reverse && typeof attr.reverse === 'string') {
                var reverseEntity = pony.entities[attr.type];
                attr.type = reverseEntity;
                attr.reverse = reverseEntity.attrs[attr.reverse];
            }
        });
    };
    Entity.prototype.getByPk = function (pk, fromDb) {
        var entity = this;
        var identityMap = entity.identityMap;
        var obj;
        if (!(pk instanceof Array)) {
            assert(!entity.compositePk);
            obj = identityMap[pk];
        } else {
            var pkLength = pk.length;
            assert(pkLength === entity.pkAttrs.length);
            if (pkLength === 1) {
                pk = pk[0];
                obj = identityMap[pk];
            } else {
                _.each(pk.slice(0, pkLength - 1), function(pkPart) {
                    if (pkPart in identityMap) {
                        identityMap = identityMap[pkPart];
                    } else {
                        identityMap = identityMap[pkPart] = {};
                    }
                });
                obj = identityMap[pk[pkLength - 1]];
            }
        }
        if (obj === undefined) {
            if (!fromDb) {
                throw new Error(entity.name + ' with primary key ' + pk + ' not found');
            }
            obj = new Instance(entity, pk);
        } else if (obj._entity_ !== entity) {
            var bases = obj._entity_.bases;
            for (var i = 0; i < bases.length; ++i) {
                if (entity === bases[i]) {
                    return obj;
                }
            }
            throw new Error(entity.name + ' with primary key ' + pk + ' not found. '
                            + obj._entity_.name + ' instance found instead');
        }
        return obj;
    };
    Entity.prototype.create = function (vals) {
        var entity = this;
        return new Instance(entity, null, vals);
    };
    pony.Entity = Entity;

    var statuses = pony.statuses = {
        created: 1,
        loaded: 2,
        modified: 3,
        inserted: 4,
        updated: 5,
        markedToDelete: 6,
        deleted: 7,
        cancelled: 8
    };
    var statusNames = pony.statusNames = {};
    _.each(statuses, function (value, name) {
        statusNames[value] = name;
    });

    var Instance = function (entity, pk, vals) {
        assert(pk !== undefined);
        var obj = this;
        obj._entity_ = entity;
        obj._id_ = pony.nextId++;
        pony.objects[obj._id_] = obj;
        var fromDb = pk !== null;
        if (!fromDb && !entity.autoPk) {
            pk = _.map(entity.pkAttrs, function (pkAttrName) {
                var pkPart = vals[pkAttrName];
                if (pkPart === undefined || pkPart === null) {
                    throw new Error('Cannot create ' + entity.name + ': primary key attribute '
                                    + pkAttrName + ' is not specified');
                }
                return pkPart;
            });
        }
        if (pk === null) {
            obj._newId_ = pony.nextNewId++;
            obj._repr_ = entity.name + '[new:' + obj._newId_ + ']';
            pony.modifiedObjects.push(obj);
        } else {
            pk = pk instanceof Array ? pk.slice(0) : [ pk ];
            var identityMap = entity.identityMap;
            var pkLength = pk.length;
            assert(pkLength === entity.pkAttrs.length);
            _.each(pk.slice(0, pkLength - 1), function(pkPart) {
                if (pkPart in identityMap) {
                    identityMap = identityMap[pkPart];
                } else {
                    identityMap = identityMap[pkPart] = {};
                }
            });
            var lastPkPart = pk[pkLength - 1];
            if (lastPkPart in identityMap) {
                throw new Error("Cannot create " + entity.name
                                + ": an instance with primary key " + pk + " already exists");
            }
            identityMap[lastPkPart] = obj;
            obj._repr_ = entity.name + '[' + pk + ']';
            pk = pkLength === 1 ? pk[0] : pk;
        }
        obj._pk_ = pk;
        obj._internalStatus_ = fromDb ? statuses.loaded : statuses.created;
        obj._status_ = ko.observable();
        obj._setStatus_(obj._internalStatus_);
        obj._dbvals_ = {};
        obj._vals_ = {};
        obj._newvals_ = {};
        obj._observables_ = {};
        _.each(entity.attrs, function (attr, attrName) {
            if (attr.isCollection) {
                obj._dbvals_[attrName] = [];
                obj._vals_[attrName] = [];
            } else if (!fromDb) {
                obj._vals_[attrName] = null;
            }
            var innerObservable = ko.observable(obj._vals_[attrName]);
            var observable = ko.pureComputed({
                read: function () {
                    //return attr.get(obj);
                    return innerObservable();
                },
                write: function (val) {
                    attr.set(obj, val);
                }
            });
            obj._observables_[attrName] = innerObservable;
            obj[attrName] = observable;
            if (attr.isCollection) {
                observable.add = function (item) {
                    attr.add(obj, item);
                };
                observable.remove = function (item) {
                    attr.remove(obj, item);
                };
                observable.create = function (vals) {
                    return attr.create(obj, vals);
                }
            }
            observable.error = ko.observable();
        });
        if (!fromDb) {
            pony.cacheModified(true);
        }
        if (vals !== undefined) {
            if (vals.constructor !== Object) {
                throw new Error("Incorrect type of 'vals' argument");
            }
            _.each(vals, function (val, attrName) {
                obj.set(attrName, val);
            });
        }
    };
    Instance.prototype.toString = function () {
        var obj = this;
        return obj._repr_;
    };
    Instance.prototype.val = function (attrName) {
        var obj = this;
        obj._entity_.checkAttrName(attrName);
        return obj._vals_[attrName];
    };
    Instance.prototype.get = function (attrName) {
        var obj = this;
        obj._entity_.checkAttrName(attrName);
        return obj[attrName];
    };
    Instance.prototype.set = function (attrName, val) {
        var obj = this;
        obj._entity_.checkAttrName(attrName);
        obj[attrName](val);
    };
    Instance.prototype.clearErrorMsg = function(attr) {
        var obj = this;
        obj[attr.name].error(undefined);
    };
    Instance.prototype.setErrorMsg = function(attr, msg) {
        var obj = this;
        obj[attr.name].error(msg);
    };
    Instance.prototype._setModified_ = function () {
        var obj = this;
        switch (obj._internalStatus_) {
            default:
                assert(false, 'Invalid status');
            case statuses.markedToDelete:
            case statuses.deleted:
            case statuses.cancelled:
                throw new Error('Object ' + String(obj) + ' was already destroyed');
            case statuses.loaded:
                pony.modifiedObjects.push(obj);
            case statuses.inserted:
            case statuses.updated:
                obj._setStatus_(statuses.modified);
                pony.cacheModified(true);
            case statuses.created:
            case statuses.modified:
        }
    };
    Instance.prototype._setStatus_ = function (status) {
        var obj = this;
        var name = statusNames[status];
        assert(name !== undefined);
        obj._internalStatus_ = status;
        obj._status_(name);
    };
    Instance.prototype.destroy = function () {
        var obj = this, entity = obj._entity_;
        if (obj.isDeleted()) {
            return; // Object was already destroyed
        }
        var prevStatus = obj._internalStatus_;
        _.each(entity.attrs, function (attr) {
            var attrName = attr.name, reverse = attr.reverse;
            var val = obj._vals_[attrName];
            if (attr.isCollection) {
                if (!reverse.isCollection && !reverse.nullable) {
                    _.each(val.slice(), function (item) {
                        item.destroy();
                    });
                }
            } else if (reverse) {
                if (val !== null && !reverse.isCollection && !reverse.nullable) {
                    val.destroy();
                }
            }
        });
        obj._setStatus_(prevStatus === statuses.created ? statuses.cancelled : statuses.markedToDelete);
        if (prevStatus === statuses.loaded) {
            pony.modifiedObjects.push(obj);
        }
        _.each(entity.attrs, function (attr) {
            var attrName = attr.name, reverse = attr.reverse;
            var val = obj._vals_[attrName];
            if (attr.isCollection) {
                if (reverse.isCollection) {
                    _.each(val, function (item) {
                        reverse.remove(item, obj, false, true);
                    });
                } else if (reverse.nullable) {
                    _.each(val, function (item) {
                        reverse.set(item, null, false, true);
                    });
                }
            } else if (reverse && val !== null && val !== obj) {
                if (reverse.isCollection) {
                    reverse.remove(val, obj, false, true);
                } else if (reverse.nullable) {
                    reverse.set(val, null, false, true);
                }
            }
        });
        pony.cacheModified(true);
    };

    Instance.prototype.isDeleted = function(){
        return this._internalStatus_ >= statuses.markedToDelete;
    };

    pony.Instance = Instance;

    pony.safe = function (obj, names, def) {
        if (obj === undefined) {
            return def;
        }
        if (!names) {
            return obj;
        }
        names = names.split('.');
        for (var i = 0; i < names.length; i++) {
            var name = names[i];
            if (obj instanceof Function) {
                obj = obj();
            }
            if (obj === null || obj === undefined) {
                return def;
            }
            obj = obj[name];
            if (obj === null || obj === undefined) {
                return def;
            }
        }
        return obj;
    };

    pony.createEntityDefinitions = function (schemaJson) {
        _.each(schemaJson, function (entityJson) {
            var entity = new Entity(entityJson);
        });
        _.each(pony.entities, function (entity) {
            entity.linkReverseAttrs();
        });
    };

    pony.createEntityObjects = function (allObjectsJson) {
        var entity, pkAttrs, maxLevel, pkVals;
        var walkObjectsJson = function (level, json, func) {
            _.each(json, function (x, pkPart) {
                pkVals.push(pkPart);
                if (level === maxLevel) {
                    func(x)
                } else {
                    walkObjectsJson(level + 1, x, func);
                }
                pkVals.pop();
            });
        };
        var processObjects = function (func) {
            _.each(allObjectsJson, function (objectsJson, entityName) {
                entity = pony.entities[entityName];
                pkAttrs = entity.pkAttrs;
                maxLevel = pkAttrs.length - 1;
                pkVals = [];
                walkObjectsJson(0, objectsJson, func);
            });
        };
        processObjects(function() {
            var obj = entity.getByPk(pkVals, true);
        });
        processObjects(function(x) {
            var obj = entity.getByPk(pkVals, true);
            _.each(x, function (val, attrName) {
                var attr = entity.attrs[attrName];
                attr.set(obj, val, true);
            });
        });
    };

    pony.convertData = function (x) {
        if (x instanceof Array) {
            return _.map(x, pony.convertData);
        }
        if (x instanceof Object) {
            var entityName = x['class'];
            if (entityName !== undefined) {
                var entity = pony.entities[entityName];
                return entity.getByPk(x['pk'], true);
            }
            var dict = {};
            _.each(x, function (val, key) {
                dict[key] = pony.convertData(val);
            });
            return dict;
        }
        return x;
    };

    pony.unmarshalData = function (json) {
        if (_.isEmpty(pony.entities)) {
            pony.createEntityDefinitions(json.schema);
        }
        pony.createEntityObjects(json.objects);
        return pony.convertData(json.data);
    };

    pony.obj2id = function (val) {
        if (val instanceof Instance) {
            return val._id_;
        }
        if (val instanceof Array) {
            return _.map(val, pony.obj2id);
        }
        return val;
    };

    pony.serialize = function (x) {
        if (x === undefined) {
            return null;
        }
        if (x instanceof Array) {
            return _.map(x, pony.serialize);
        }
        if (x instanceof Instance) {
            return {_id_: x._id_, _pk_: x._pk_, 'class': x._entity_.name};
        }
        if (x.prototype === Object) {
            var result = {};
            _.each(x, function (val, key) {
                result[key] = pony.serialize(val);
            });
            return result;
        }
        return x;
    };

    var modifiedStatuses = {};
    modifiedStatuses[statuses.created] = 'c';
    modifiedStatuses[statuses.modified] = 'u';
    modifiedStatuses[statuses.markedToDelete] = 'd';

    pony.getChanges = function (data) {
        data = pony.serialize(data);
        var objects = [];
        var obj2id = pony.obj2id;
        _.each(pony.modifiedObjects, function (obj) {
            var status = modifiedStatuses[obj._internalStatus_];
            if (!status) {
                return; // no need to save this object
            }
            var data = {'class': obj._entity_.name, _id_: obj2id(obj), _status_: status};
            if (obj._pk_ !== null) {
                data._pk_ = obj._pk_;
            }
            if (status !== 'd') {
                _.each(obj._newvals_, function (val, attrName) {
                    var old = obj2id(obj._dbvals_[attrName]);
                    if (old === val) {
                        return; // attribute value was changed and then returned back
                    }
                    var attr = obj._entity_.attrs[attrName];
                    if (old === undefined) {
                        data[attrName] = obj2id(val);
                    } else if (!attr.isCollection) {
                        data[attrName] = { 'old': obj2id(old), 'new': obj2id(val) };
                    } else {
                        old = obj2id(old);
                        val = obj2id(val);
                        var added = _.difference(val, old);
                        var removed = _.difference(old, val);
                        if (added.length || removed.length) {
                            var diff = {};
                            if (added.length) {
                                diff.added = added;
                            }
                            if (removed.length) {
                                diff.removed = removed;
                            }
                            data[attrName] = diff;
                        }
                    }
                });
            }
            objects.push(data);
        });
        return {data: data, objects: objects};
    };

}(window.pony = window.pony || {}));