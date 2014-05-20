
var async = require("async"),
	Stream = require("stream"),
	_ = require("underscore");

module.exports = {

	checkKeys: function(values){
		var keys = (values.RowKey || false) && (values.PartitionKey || false);
		if(!keys){
			return new Error("Invalid RowKey or PartitionKey provided!");
		}else{
			// TODO fix this, but cant do looping with callback because it'll need to break
			// also this function is synchronous
			var i, _keys = Object.keys(values);
			for(i = 0; i < _keys.length; i++){
				if(values[_keys[i]] && values[_keys[i]].autoIncrement)
					return new Error("Azure does not support auto incrementing IDs. Make sure your collection definition has 'autoPK' set to false. Try using a GUID generator instead and use that as your RowKey. Make sure one of your collection's attributes has the property 'primaryKey' set to true as well. Technically RowKeys only have to be unique within partitions, but generating your RowKey in such a way that it's globally unique and setting that as your primaryKey fixes both problems.");
			}
		}
		return true;
	},

	getPages: function(results, continuation, callback){
		var self = this;
		continuation.getNextPage(function(error, entities, newContinuation){
			results = results.concat(entities);
			if(newContinuation.hasNextPage()){
				self.getPages(results, newContinuation, callback);
			}else{
				callback(results);
			}
		});
	},

	streamPages: function(continuation, stream){
		var self = this;
		continuation.getNextPage(function(error, entities, newContinuation){
			if(newContinuation.hasNextPage()){
				stream.write(entities)
				self.getPages(newContinuation, stream);
			}else{
				stream.write(entities);
				stream.end();
			}
		});
	},

	cleanAzureMetadata: function(set, callback){
		async.map(set, function(curr, cb){
			delete curr._;
			cb(null, curr);
		}, function(err, cleaned){
			if(err)
				callback(err);
			else
				callback(null, cleaned);
		});
	},

	// convert the key/value pairs in the query @options to azure TableQuery methods
	// in this method @key and @value represent one top level key/value pair from @options
	// right now it assumes each key/value pair should be merged with an AND operation
	// if anybody wants to use an OR instead you can provide it as an optional last parameter
	filterQuery: function(query, key, value, logic){
		var i, logic = typeof logic !== "undefined" ? logic : "and";
		var cmd = logic;
		switch(key){
			case "where":
				// loop in blocking way to ensure cmd is used properly
				var keys = Object.keys(value);
				for(i = 0; i < keys.length; i++){
					var k = keys[i];
					if(query._where.length === 0)
						cmd = "where";
					if(_.isObject(value[k])){
						var str = k + " ";
						var op = Object.keys(value[k])[0];
						query = query[cmd](str + op.replace(/^=$/, "eq") + " ?", value[k][op]);
					}else{
						var str = k + " eq ?";
						query = query[cmd](str, value[k]);
					}
					if(cmd == "where") cmd = logic;
				}
			break;
			case "limit": 
				query = query.top(value);
			break;
			default:
				console.log("Invalid filter provided for sails-azure. Key: ", key);
			break;
		}
		return query;
	},

	// TODO implement these in app code or just disallow their use??
	checkOptions: function(options){
	    var unsupported = [
	        "groupBy",
	        "sum",
	        "average",
	        "min", 
	        "max",
	        "sort"
	    ];
	    // be lazy for now
	    var bad = _.find(unsupported, function(curr){
	      return options[curr] || false; // to boolean
	    });
	    return typeof bad === "undefined";
	},

	SimpleStream: function(onData, onEnd){
		var stream = new Stream();
		stream.setEncoding("utf8");
		stream.on("data", function(data){
			try{
				data = JSON.parse(data);
			}catch(e){
				console.log("Error parsing JSON for SimpleStream", e);				
			}finally{
				onData(data);
			}
		});
		stream.on("end", onEnd);
		stream.on("error", function(err){
			console.log("Error with SimpleStream!", err);
			this.end();
		});
		stream.end = function(){
			this.emit("end");
		};
		stream.write = function(data){
			if(_.isObject(data))
				data = JSON.stringify(data);
			this.emit("data", data);
		};
		return stream;
	},

};
