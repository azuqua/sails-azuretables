
var async = require("async"),
    _ = require("underscore"),
    azure = require("azure"),
    generic_pool = require("generic-pool"),
    utils = require("./utils");

module.exports = (function(){
  var pool = false;

  // allow this to be done when the server starts so config info is available
  var createConnectionPool = function(collection){
    pool = generic_pool.Pool({
      name: "azure",
      log: collection.log || false,
      max: collection.maxServices || 9,
      idleTimeoutMillis : 900000, // 15 minutes
      create: function(callback){
        var ts = null;
        try{
            if(collection.development){
              ts = azure.createTableService("UseDevelopmentStorage=true");
            }else{
              var retryOperations = collection.retryOperations || "ExponentialRetryPolicyFilter";
              retryOperations = new azure[retryOperations]();
              ts = azure.createTableService(collection.account, collection.secret).withFilter(retryOperations);
            }
        }catch(e){
          callback(e);
        }
        callback(null, ts);
      },
      destroy: function(conn){
        // Azure services communicate over OData and don't need to be explicitly closed
        conn = null;
      }
    });
  };

  // stores RowKey and PartitionKey references for each model
  var schema = {};

  var adapter = {
    syncable: false,
    autoPK: false,
    defaults: {
      schema: false,
      autoPK: false,
      migrate: 'alter'
    },

    // This method runs when a model is initially registered at server start time
    registerCollection: function(collection, cb) {

      collection.account = typeof collection.config !== "undefined" ? collection.config.account : collection.account;
      collection.secret = typeof collection.config !== "undefined" ? collection.config.secret : collection.secret;

      if(!collection.account || !collection.secret){
        console.log("Invalid Azure authentication info provided. Using local development storage instead.");
        collection.development = true;
      }

      // create the connection pool at server start so config options are available
      if(!pool)
        createConnectionPool(collection);

      schema[collection.identity] = collection.definition;

      var tname = collection.tableName || collection.identity;

      this.define(tname, collection.definition, function(err, name){
        if(err){
          cb(err);
        }else{
          cb();
        }
      });
    },

    teardown: function(cb) {
      pool.drain(function() {
        pool.destroyAllNow();
        cb();
      });
    },

    define: function(collectionName, definition, cb) {
      pool.acquire(function(err, ts){
        if(err){
          cb(err);
        }else{
          ts.createTableIfNotExists(collectionName, function(error){
            pool.release(ts);
            if(error)
              cb(error);
            else
              cb(null, collectionName);
          });
        }
      });
    },

    describe: function(collectionName, cb) {
      // Azure tables are schemaless so keep the original collection attributes locally and return them here    
      if(schema[collectionName]){
        cb(new Error("Schema not found for " + collectionName));
      }else{
        cb(null, schema[collectionName])
      }
    },

    drop: function(collectionName, cb) {
      pool.acquire(function(err, ts){
        if(err){
          cb(err);
        }else{
          ts.deleteTable(collectionName, function(error){
            pool.release(ts);
            if(error){
              cb(error);
            }else{
              cb(null, collectionName);
            }
          });
        }
      });
    },

    create: function(collectionName, values, cb) {
      var check = utils.checkKeys(values);
      if(check instanceof Error){
        cb(check);
      }else{
        pool.acquire(function(err, ts){
          if(err){
            cb(err);
          }else{
            ts.insertOrMergeEntity(collectionName, values, function(error){
              pool.release(ts);
              if(error)
                cb(error);
              else
                cb(null, values);
            });
          }
        });
      }
    },

    createEach: function(collectionName, data, cb){
      if(!_.isArray(data))
        data = [data];
      if(data.length < 1)
        return cb(null, data);
      pool.acquire(function(err, ts){
        if(err){
          cb(err);
        }else{
          ts.beginBatch();
          async.map(data, function(curr, callback){
            var check = utils.checkKeys(curr);
            if(check instanceof Error){
              callback(check);
            }else{
              ts.insertOrMergeEntity(collectionName, curr, function(error){
                if(error){
                  callback(error);
                }else{ 
                  callback(null, curr);
                }
              });
            }
          }, function(error, results){
            if(error){
              pool.release(ts);
              cb(error);
            }else{
              ts.commitBatch(function(_error){
                pool.release(ts);
                if(_error){
                  cb(_error);
                }else{
                  cb(null, results);
                }
              });
            }
          });
        }
      });
    },

    find: function(collectionName, options, cb) {
      var check = utils.checkOptions(options);
      if(!check){
        cb(new Error("Sorry, sails-azure doesn't natively support the '" + bad + "' operation. For the time being if you'd like to use this operation you'll have to implement it in your application logic."));
      }else{
        // TODO inspect options to filter results properties
        var query = azure.TableQuery.select().from(collectionName);
        var properties = Object.keys(options);
        //console.log("properties: ", properties);
        async.each(properties, function(prop, callback){
          //console.log("Interating over properties: ", prop);
          query = utils.filterQuery(query, prop, options[prop]);
          callback();
        }, function(err){
          if(err){
            cb(err);
          }else{
           // console.log("Find query: ", query);
            // run the query
            pool.acquire(function(error, ts){
              if(error){
                cb(error);
              }else{
                //console.log("Find ts: ", ts);
                ts.queryEntities(query, function(e, result, continuation){
                  if(e){
                    pool.release(ts);
                    cb(e);
                  }else{
                    if(continuation && continuation.hasNextPage()){
                      utils.getPages(result, continuation, function(allResults){
                        pool.release(ts);
                        utils.cleanAzureMetadata(allResults, cb);
                      });
                    }else{
                      pool.release(ts);
                      utils.cleanAzureMetadata(result, cb);
                    }
                  }
                });
              }
            });
          }
        });
      }
    },

    // update the result set for @options in one batch
    update: function(collectionName, options, values, cb){
      var check = utils.checkOptions(options);
      var self = this;
      var keys = Object.keys(values);
      if(!check){
        cb(new Error("Sorry, sails-azure doesn't natively support the '" + bad + "' operation. For the time being if you'd like to use this operation you'll have to implement it in your application logic."));
      }else{
        self.find(collectionName, options, function(err, results){
          if(err){
            cb(err);
          }else if(!results || (results instanceof Array && results.length < 1)){
            cb(null, []);
          }else{
            pool.acquire(function(_err, ts){
              if(_err){
                cb(_err);
              }else{
                ts.beginBatch();
                async.map(results, function(result, callback){
                  keys.forEach(function(key){
                    result[key] = values[key];
                  });
                  ts.mergeEntity(collectionName, result, function(_e){
                    if(_e){
                      callback(_e);
                    }else{
                      callback(null, result);
                    }
                  });
                }, function(_error, updated){
                  if(_error){
                    pool.release(ts);
                    cb(_error);
                  }else{ 
                    ts.commitBatch(function(_error){
                      pool.release(ts);
                      if(_error){
                        cb(_error);
                      }else{
                        cb(null, updated);
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    },

    destroy: function(collectionName, options, cb) {
      var check = utils.checkOptions(options);
      var self = this;
      if(!check){
        cb(new Error("Sorry, sails-azure doesn't natively support the '" + bad + "' operation. For the time being if you'd like to use this operation you'll have to implement it in your application logic."));
      }else{
        self.find(collectionName, options, function(err, results){
          if(err){
            cb(err);
          }else if(!results || (results instanceof Array && results.length < 1)){
            cb(null, []);
          }else{
            pool.acquire(function(e, ts){
              if(e){
                cb(e);
              }else{
                ts.beginBatch();
                async.each(results, function(curr, callback){
                  var keys = {
                    RowKey: curr.RowKey,
                    PartitionKey: curr.PartitionKey
                  };
                  ts.deleteEntity(collectionName, keys, function(_error){
                    if(_error){
                      callback(_error);
                    }else{
                      callback();
                    }
                  });
                }, function(error){
                  if(error){
                    pool.release(ts);
                    cb(error);
                  }else{
                    ts.commitBatch(function(_err){
                      pool.release(ts);
                      if(_err){
                        cb(_err);
                      }else{
                        cb();
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    },

    // THIS ISNT TESTED!
    // do exactly what find() does but write the objects when they come in instead of adding them to the overall result set
    stream: function(collectionName, options, stream) {
      var check = utils.checkOptions(options);
      if(!check){
        stream.end();
        return new Error("Sorry, sails-azure doesn't natively support the '" + bad + "' operation. For the time being if you'd like to use this operation you'll have to implement it in your application logic.");
      }else{
        // TODO inspect options to filter results properties
        var query = azure.TableQuery.select().from(collectionName);
        var properties = Object.keys(options);
        async.each(properties, function(prop, callback){
          query = utils.filterQuery(query, prop, options[prop]);
        }, function(err){
          if(err){
            stream.end();
            throw err;
          }else{
            // run the query
            pool.acquire(function(error, ts){
              if(error){
                stream.end();
                throw error;
              }else{
                ts.queryEntities(query, function(e, result, continuation){
                  if(e){
                    pool.release(ts);
                    stream.end();
                    throw e;
                  }else{
                    if(continuation && continuation.hasNextPage()){
                      utils.cleanAzureMetadata(results, function(cleaned){
                        stream.write(cleaned);
                      });
                      utils.streamPages(continuation, stream);
                    }else{
                      pool.release(ts);
                      utils.cleanAzureMetadata(results, function(cleaned){
                        stream.write(cleaned);
                        stream.end();
                      })
                    }
                  }
                });
              }
            });
          }
        });
      }
    }
   
  };
  return adapter;
})();
