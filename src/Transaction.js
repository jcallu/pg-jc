var Q = require('q');
var AbstractTable = require('./AbstractTable.js');
var _ = require('lodash');
var PGNativeAsync = require('pg').native
var ConnectionClient = require('./ConnectionClient')


function Transaction (databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnection){
  var fsSchemaCacheKey = AbstractTable.getConnectionFSCacheKey(databaseName,databaseAddress,databasePort,databaseUser)
  var tableSchema =  process[fsSchemaCacheKey];
  AbstractTable.createVirtualSchema(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnection);
  if(! tableSchema ){
    throw Error("Schema "+databaseName+" not initialized");
  }
  this.TransactionParams = {}
  this.TransactionParams.databaseName = databaseName;
  this.TransactionParams.databaseAddress = databaseAddress;
  this.TransactionParams.databasePassword = databasePassword
  this.TransactionParams.databasePort = databasePort;
  this.TransactionParams.databaseUser = databaseUser;
  this.TransactionParams.dbConnection = dbConnection;
  this.TransactionParams.dbConnectionString = dbConnection.getConnectionString()
  var client = new PGNativeAsync.Client( this.TransactionParams.dbConnectionString );
  var dbConnectionClient = new ConnectionClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,client)
  var tables = {};
  _.forEach(tableSchema,function(value,tablename){
    tables[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnectionClient);
  });
  this.Q = Q;
  this.TransactionParams.rolledback = false;
  this.TransactionParams.closed = false;
  this.TransactionParams.begun = false
  this.Tables = {}
  for( var table in tables ){
    this.Tables[table] = tables[table];
  }
  this.PGClient = transactionDBConnection;
}

Transaction.prototype.getClient = function(){
  return this.PGClient;
};

Transaction.prototype.getDB = function(){
  return this.TransactionParams.databaseName;
};


Transaction.prototype.begin = function(){

  // var s = process.hrtime();
  var q = Q.defer();
  var self = this;

  var beginQuery = "BEGIN;";
  self.PGClient.query(beginQuery,function(err,ret){
    // console.log("BEGIN",err,ret)
    if(err) {
      self.rollback();
    } else {
      self.TransactionParams.begun = true;
      q.resolve();
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
  // var s = process.hrtime();
  var self = this;
  var q = Q.defer();
  var commitQuery = "COMMIT;";
  self.PGClient.query(commitQuery,function(err,ret){
    self.PGClient.logQuery(s,commitQuery);
    if(err){
      self.rollback(err);
    } else {
      self.PGClient.end();
      self.TransactionParams.closed = true;
      q.resolve(ret);
    }

  });
  return q.promise;
};


var ROLLBACK_MSG = "<~ Transaction Client Closed And Rolled Back";


Transaction.prototype.rollback = function(err){
  var self = this;

  // var s = process.hrtime();
  var q = Q.defer();
  var rollbackQuery = "ROLLBACK;";

  if( self.TransactionParams.rolledback == false ) {
    self.PGClient.query(rollbackQuery,function(err2,ret){
      self.TransactionParams.rolledback = true;
      self.PGClient.end();
      self.TransactionParams.closed = true;
      if(err) {
        err.message += " FAILURE: Transaction ERROR " + ROLLBACK_MSG;
      }
      else if(err2){
        err2.message += " FAILURE: Client Query ERROR " + ROLLBACK_MSG;
        err = err2;
      }
      else {
        err = new Error('User Rollback');
        err.stack = 'NOTICE: Transaction Rolled Back by User';
      }
      q.reject(err);
    });
  } else {
    try {
      self.PGClient.end();
      self.TransactionParams.closed = true;
    } catch(e){

    }
    q.reject(null);
  }
  return q.promise;
};

module.exports = Transaction;
