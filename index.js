'use strict';
const promise = require("bluebird");
const aerospike = require("aerospike");
const filter = aerospike.filter;
const GeoJSON = aerospike.GeoJSON

const async = require("async");
const _ = require("lodash");

var oASClientList = {};

var AddClient = function(){
  var sName = "default";
  var arrsHost = false;

  if(arguments.length == 1){
    if(oASClientList["default"])
      return;

    arrsHost = arguments[0];

  }else{
    if(oASClientList[arguments[0]])
      return;

    sName = arguments[0];
    arrsHost = arguments[1];
  }

  if(arrsHost.constructor !== Array)
    throw "Hosts needs to be an array";

  oASClientList[sName] = aerospike.client({
    hosts: arrsHost.join(","),
    maxConnsPerNode: 1000,
    log: {
      level: aerospike.log.ERROR,
      file: "app.log"
    }
  })
}

var filters = {
  range: function(bin,min,max){
    return filter.range(bin,min,max);
  },
  equal: function(bin,value){
    return filter.equal(bin,value);
  },
  contains: function(bin,value){
    return filter.contains(bin,value, aerospike.indexType.LIST);
  },
  geo: {
    radius: function(bin,lon,lat,radius){
      return filter.geoWithinRadius(bin,lon,lat,radius)
    }
  }
}

var IndexTypes = {
  "INT": "createIntegerIndex",
  "STR": "createStringIndex",
  "GEO": "createGeo2DSphereIndex"
}

var connect = function(){
  var sName = arguments[0] || "default";
  if(!oASClientList[sName])
    throw "Could not find client '"+sName+"'";

  return new Promise(function(resolve,reject){
    oASClientList[sName].connect(function(err,response) {
      return err?reject(err):resolve(response);
    });
  })
}

var Aerospike = function(set,ns){
  this.oASClient = oASClientList["default"] || null;
  this.ns = ns || "primary";

  if(!set)
    throw "You need to use a set";

  this.set = set;
}

Aerospike.prototype = {
  use: function(sName){
    if(!oASClientList[sName])
      throw "Could not find client '"+sName+"'";

    this.oASClient = oASClientList[sName]
  },
  createArrayIndex : function(bin,type,isAsync){
    return this.createIndex(bin,type,isAsync,true);
  },
  removeArrayIndex : function(bin){
    return this.removeIndex(bin,true);
  },
  removeIndex: function(bin,bIsArray){
    var self = this;
    var sIndexName = this.set+"_"+bin+(bIsArray?"_list":"")
    return new Promise(function(resolve,reject){
      self.oASClient.indexRemove(self.ns, sIndexName, function (err) {
        if (err){
          if(err.code == aerospike.status.ERR_INDEX_NOT_FOUND)
            return resolve(false);

          return reject(err)
        }
        return resolve(true);
      });
    });
  },
  createIndex : function(bin,type,isAsync,bIsArray) {
    if(!IndexTypes[type])
      return Promise.reject("createIndex: Index type must be INT, STR or GEO")

    isAsync = isAsync || false;

    var self = this;
    var options = {
      ns: this.ns,
      set: this.set,
      bin: bin,
      index: this.set+"_"+bin+(bIsArray?"_list":"")
    }

    if(bIsArray)
      options.type = aerospike.indexType.LIST;

    return new Promise(function(resolve,reject){
      self.oASClient[IndexTypes[type]](options, function (err, job) {
        if (err){
          if(err.code == aerospike.status.ERR_INDEX_FOUND)
            return resolve();

          return reject(err);
        }

        if(isAsync){
          resolve();
        }else{
          job.waitUntilDone(100, function (err) {
            if (err)
              return reject(err);
            resolve();
          });
        }
      });
    });
  },
  //filter.geoWithinRadius("geobin",lat,lon,radius)
  //filter.range('i', 0, 10000)
  //filter.equals("bin","value")
  must: function(){
    var args = Array.prototype.slice.call(arguments);
    args.unshift("AND")
    return this.search.apply(this, args)
  },
  should: function(){
    var args = Array.prototype.slice.call(arguments);
    args.unshift("OR")
    return this.search.apply(this, args)
  },
  search: function(){
    if(arguments.length==1){
      var type="AND"
      var arrofilters = [arguments[0]]
    }else{
      var type = arguments[0];
      var arrofilters = Array.prototype.slice.call(arguments).slice(1);
    }

    var arrayArrRecords = [];
    var arrayErrors = [];
    var self = this;

    return new Promise(function(resolve,reject){
      var arrRecords = [];

      async.each(arrofilters,function(filter,cb){
        var query = self.oASClient.query(self.ns, self.set, { filters: [filter] })

        var arroRecords = [];
        var stream = query.foreach(null,null,function(error){
          reject(error);
        });

        stream.on('data', function (record) {
          arroRecords.push(record);
        })
        stream.on('error', function (error) {
          arrayErrors.push(error);
        })
        stream.on('end', function () {
          arrayArrRecords.push(arroRecords);
          cb();
        })
      },function(){
        if(arrayErrors.length)
          return reject(arrayErrors);

        if(type=="AND"){
          arrayArrRecords.push(function(x,y){
            return x.key.digest.equals(y.key.digest);
          })

          arrRecords = _.intersectionWith
                        .apply(null, arrayArrRecords)

        }else if(type=="OR"){
          arrRecords = _.uniq(_.concat.apply(null,arrayArrRecords));
        }

        resolve(arrRecords.map(function(o){return o.bins}))
      });
    });
  },
  remove : function(key){
    var self = this;
    var oKey = {
        ns: this.ns,
        set: this.set,
        key: key
    };

    return new Promise(function(resolve,reject){
      self.oASClient.remove(oKey,function(err, rec, meta){
        if(err){
          return reject(err);
        }

        return resolve(true)
      });
    });
  },
  read : function(){
    var self = this;
    var arrsKeys =  Array.prototype.slice.call(arguments);

    arrsKeys = arrsKeys.map(function(sKey){
      return {key: new aerospike.Key(self.ns, self.set,sKey), read_all_bins: true}
    })

    return new Promise(function(resolve,reject){
      self.oASClient.batchRead(arrsKeys, function (error, results) {
        if (error) {
          return reject(error)
        }

        var arrsRecords = [];
        results.forEach(function (result) {
          switch (result.status) {
            case aerospike.status.AEROSPIKE_OK:
              arrsRecords.push(result.record.bins)
              break
          }
        });

        if(arrsKeys.length==1){
          resolve(arrsRecords.length?arrsRecords[0]:false)
        }else{
          resolve(arrsRecords)
        }
      })
    });
  },
  write: function(key,value,meta){
    var self = this;
    var oKey = {
        ns: this.ns,
        set: this.set,
        key: key
    };

    return new Promise(function(resolve,reject){
      let policy = new aerospike.WritePolicy({
        key: aerospike.policy.key.SEND
      })

      self.oASClient.put(oKey, value, meta || {},policy,function(err, rec) {
        if(err)
          return reject(err)

        resolve(rec);
      });
    })
  },
  readKeys: function(){
    return this.readAll(true);
  },
  readAll: function(keysOnly){
    var scan = this.oASClient.scan(this.ns,this.set)
    scan.concurrent = true
    scan.nobins = keysOnly?true:false;

    return new Promise(function(resolve,reject){
      var records=[];
      var stream = scan.foreach()
      stream.on('data', function (record) {
        records.push(keysOnly?record.key:record.bins);
      })
      stream.on('error', function (error) {
        reject(error)
      })
      stream.on('end', function () {
        resolve(records)
      })
    })
  }
}

module.exports = {
  AddClient : AddClient,
  filter:filters,
  connect: connect,
  GeoPoint: GeoJSON.Point,
  Query : function(set,ns){
     return new Aerospike(set,ns)
  }
}
