
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
      idleTimeoutMillis : 300000, // 5 minutes
      create: function(callback){
        var ts = null;
        try{
            if(collection.development){
              ts = azure.createTableService("UseDevelopmentStorage=true");
            }else{
              var retryOperations = collection.retryOperations || "ExponentialRetryPolicyFilter";
              retryOperations = new azure[retryOperations]();
              ts = azure.createTableService(collection.config.account, collection.config.secret).withFilter(retryOperations);
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

      _.each(collection._instanceMethods, function(v, k, list){
        if(!collection[k])
          collection[k] = v;
      });

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
            ts.insertEntity(collectionName, values, function(error){
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
              ts.insertEntity(collectionName, curr, function(error){
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

    /*
      Stream the result set that corresponds to @options in batches of 1000 (that's all Azure returns in one request anyways)
      Update batches as they come in and commit the changes to Azure asyncronously.
      The upside to this approach is that it allows for concurrent updates on data from the same overall result set.
      The downside is that it makes error reporting really difficult.
      This is about as far from consistent or atomic as you can get but Azure tables aren't really ACID compliant anyways.
    */
    updateStream: function(collectionName, options, values, cb) {
      var check = utils.checkOptions(options);
      var self = this;
      var keys = Object.keys(values);
      if(!check){
        cb(new Error("Sorry, sails-azure doesn't natively support the '" + bad + "' operation. For the time being if you'd like to use this operation you'll have to implement it in your application logic."));
      }else{
        // use stream() in case the result set is very large
        // if it's greater than 1000 then this will update the elements in batches of 1000
        var allResults = [],
            end = false, // mutex 
            processing = 0; // kinda like a semaphore
        var jstream = new utils.SimpleStream(function(resultSet){
          var stream = this;
          // callback for on("data")
          pool.acquire(function(err, ts){
            if(err){
              cb(err);
            }else{
              processing++;
              ts.beginBatch();
              if(!_.isObject(resultSet))
                resultSet = JSON.parse(resultSet);
              async.map(resultSet, function(result, callback){
                keys.forEach(function(key){
                  result[key] = values[key];
                });     
                ts.updateEntity(collectionName, result, function(_e){
                  if(_e){
                    callback(_e);
                  }else{
                    callback(null, result);
                  }
                });           
              }, function(err, updated){
                if(err){
                  pool.release(ts);
                  cb(err);
                }else{
                  ts.commitBatch(function(_err){
                    pool.release(ts);
                    if(_err){
                      // TODO need better error handling here
                      processing--;
                      console.log("There was an error updating a batch of records. The starting RowKey and PartitionKey are: ", updated[0].RowKey, updated[0].PartitionKey);
                    }else{
                      allResults = allResults.concat(updated);
                      processing--;
                    }
                    // check if the stream has already ended and that this is the last batch still processing
                    // if so then call the final callback with all of the results
                    // if not then we can assume another batch is still processing or there's more data incoming
                    if(processing == 0 && end){
                      cb(null, allResults)
                    }
                  });
                }
              });
            }
          });
        }, function(){
          // callback for on("end")
          if(processing > 0){
            // super hacky way of asyncronously waiting for the rest of the results to load
            // use a sempahore for each update batch and a mutex for the "ended" state
            end = true;
          }else{
            cb(null, allResults);
          }
        });
        self.stream(collectionName, options, jstream);
      }
    },

    // update the result set for @options in one batch
    // at least this is closer to being atomic and a bit closer to being consistent than updateStream
    // it's still far from being truly ACID compliant
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
                  ts.updateEntity(collectionName, result, function(_e){
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
