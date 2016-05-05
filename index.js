var AbstractTable = require(__dirname+'/src/AbstractTable.js');
var parseDBConnectionString = require(__dirname+'/src/parseDBConnectionString.js');
var util = require('util');
var Transaction = require(__dirname+'/src/Transaction.js');
var Connection = require(__dirname+'/src/Connection.js')
var _  = require('lodash')
module.exports = function(connStr){
  var orm = {};
  var config = parseDBConnectionString(connStr);
  config = config instanceof Object ? config : {}
  var databaseName = process.env.PGDATABASE || config.database || 'template1';
  var databaseAddress = process.env.PGHOST || config.host || '127.0.0.1';
  var databasePassword = process.env.PGPASSWORD || config.password || '';
  var databasePort = process.env.PGPORT || config.port || 5432;
  var databaseUser = process.env.PGUSER || config.user || 'postgres';

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
  var connection = new Connection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser)
  AbstractTable.createVirtualSchema(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  _.forEach(process[databaseName.toUpperCase()+"_TABLES_SCHEMA_CACHE"],function(value,tablename){
    orm[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  });
  function TransactionWrapper(){
    Transaction.call(this,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,connection);
  }
  util.inherits(TransactionWrapper,Transaction);
  orm.Transaction = TransactionWrapper;
  orm.promise = function(){ var q = Q.defer(); q.resolve(undefined); return q.promise; };
  orm.PGClient = connection
  return orm;
};
