var AbstractTable = require(__dirname+'/src/AbstractTable.js');
var parseDBConnectionString = require(__dirname+'/src/parseDBConnectionString.js');
var util = require('util');
var Transaction = require(__dirname+'/src/Transaction.js');

module.exports = function(connStr){
  var database = {};
  var config = parseDBConnectionString(connStr);
  var databaseName = config.database;
  var databaseAddr = config.host;
  AbstractTable.createVirtualSchema(databaseName,databaseAddr);
  var databases = process[databaseName.toUpperCase()+"_TABLES_SCHEMA_CACHE"] || {};
  for( var tablename in databases ){
    if( typeof databases[tablename] === 'object' ){
      database[tablename] = new AbstractTable(tablename,databaseName,databaseAddr);
    } else {
      var e = new Error("missing schema");
      throw e;
    }
  }

  function TransactionWrapper(){
    Transaction.call(this,databaseName,databaseAddr);
  }
  util.inherits(TransactionWrapper,Transaction);
  database.Transaction = TransactionWrapper;
  return database;
};
