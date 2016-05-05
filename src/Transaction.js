var Q = require('q');
var AbstractTable = require(__dirname+'/AbstractTable.js');
var connection = AbstractTable.connection;

function Transaction (databaseName,databaseAddr){
  if(! process[databaseName.toUpperCase()+"_TABLES_SCHEMA_CACHE"] ){
    throw Error("Schema "+databaseName+" not initialized");
  }
  this.databaseName = databaseName;
  this.databaseAddr = databaseAddr;
  this[databaseName+'db'] = {};

  AbstractTable.createVirtualSchema(databaseName,databaseAddr);

  var client = new connection.DBClient.Client(connection.conString(this.databaseName,this.databaseAddr));
  var dbtmp = {};
  var dbCached = process[databaseName.toUpperCase()+"_TABLES_SCHEMA_CACHE"] || {}
  dbCached.forEach(function(value,tablename){
    dbtmp[tablename] = new AbstractTable(tablename,databaseName,databaseAddr,client);
  });

  this.Q = Q;
  this.rolledback = false;
  this[databaseName+'db'] = dbtmp;
  this.client = client;
}

Transaction.prototype.getClient = function(){
  return this.client;
};

Transaction.prototype.getDB = function(){
  return this.db;
};

Transaction.prototype.begin = function(){
  var s = process.hrtime();
  var q = Q.defer();
  var self = this;

  self.client.connect(function(err){
    if(err) {
      q.reject(err);
    } else {
      var beginQuery = "BEGIN;";
      self.client.query(beginQuery,function(err,ret){
        connection.logQuery(s,beginQuery);
        if(err) {
          self.rollback();
        } else {
          q.resolve(self);
        }
      });
    }
  });
  return q.promise;
};
Transaction.prototype.promise = function(){
  return Q.fcall(function(){ return; });
};

Transaction.prototype.boundary = function(promisedStep){
  var self = this;
  var q = Q.defer();
  if( !( promisedStep instanceof Object ) && promisedStep.state !== 'pending' ) {
    self.rollback().fail(function(err){
      q.reject(new Error("Boundary function was not a promise"));
    });
  } else {
    promisedStep.then(function(ret){
      q.resolve(ret);
    })
    .fail(function(err){
      self.rollback(err).fail(function(err){
        q.reject(err);
      });
    }).done();
  }

  return q.promise;
};

Transaction.prototype.commit = function(){
  var s = process.hrtime();
  var self = this;
  var q = Q.defer();
  var commitQuery = "COMMIT;";
  self.client.query(commitQuery,function(err,ret){
    connection.logQuery(s,commitQuery);
    if(err){
      self.rollback(err);
    } else {
      self.client.end();
      q.resolve(ret);
    }
  });
  return q.promise;
};


var ROLLBACK_MSG = " <~ Closed client and rolled back";


Transaction.prototype.rollback = function(err){
  var self = this;

  var s = process.hrtime();
  var q = Q.defer();
  var rollbackQuery = "ROLLBACK;";

  if( !self.rolledback ) {
    self.client.query(rollbackQuery,function(err2,ret){
      self.rolledback = true;
      connection.logQuery(s,rollbackQuery);
      self.client.end();
      if(err) {
        err.message += " Transaction ERROR " + ROLLBACK_MSG;
      }
      else if(err2){
        err2.message += " Client Query ERROR " + ROLLBACK_MSG;
        err = err2;
      }
      else {
        err = new Error('USER ROLLBACK FAILSAFE '+ROLLBACK_MSG);
      }
      q.reject(err);
    });
  } else {
    self.client.end();
    q.resolve();
  }
  return q.promise;
};

module.exports = Transaction;
