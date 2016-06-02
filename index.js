var Q = require('q')
var AbstractTable = require(__dirname+'/src/AbstractTable.js');
var parseDBConnectionString = require(__dirname+'/src/parseDBConnectionString.js');
var util = require('util');
var Transaction = require(__dirname+'/src/Transaction.js');
var ConnectionClient = require(__dirname+'/src/ConnectionClient.js');
var ConnectionPoolAndSyncClient = require(__dirname+'/src/ConnectionPoolAndSyncClient.js')
var _  = require('lodash')
module.exports = function(connStr){
  var orm = {};
  // orm.Classes = {
  //   ConnectionPoolAndSyncClient: ConnectionPoolAndSyncClient,
  //   Transaction: Transaction,
  //   ConnectionClient: ConnectionClient,
  //   parseDBConnectionString: parseDBConnectionString,
  //   AbstractTable: AbstractTable
  // }
  var config = parseDBConnectionString(connStr);
  config = config instanceof Object ? config : {}
  var databaseName =  config.database || process.env.PGDATABASE || 'template1';
  var databaseAddress =   config.host || process.env.PGHOST || '127.0.0.1';
  var databasePassword = config.password || process.env.PGPASSWORD ||  '';
  var databasePort = config.port ||  process.env.PGPORT || 5432;
  var databaseUser = config.user ||  process.env.PGUSER || 'postgres';

  if( ! databaseName ) {
    var err = new Error("database name env PGDATABASE or in connection string invalid")
    throw err
  }
  if( ! databaseAddress ) {
    var err = new Error("database host ip env PGHOST or in connection string invalid")
    throw err
  }
  if( ! databasePort ) {
    var err = new Error("database port in connection string invalid")
    throw err
  }
  if( ! databaseUser ) {
    var err = new Error("database user in connection string invalid")
    throw err
  }
  var connection = new ConnectionPoolAndSyncClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser)
  var connectionSchemaKey = AbstractTable.getConnectionFSCacheKey(databaseName,databaseAddress,databasePort,databaseUser)
  AbstractTable.createVirtualSchema(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  _.forEach(process[connectionSchemaKey],function(value,tablename){
    orm[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  });
  function TransactionBoundary(){
    Transaction.call(this,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  }
  util.inherits(TransactionBoundary,Transaction);
  orm.Transaction = TransactionBoundary;
  orm.Promise = function(){ var q = Q.defer(); q.resolve(undefined); return q.promise; };
  orm.PGClient = connection

  return orm;
};
