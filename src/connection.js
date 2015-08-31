/**
 * @module connection
 * @author Jacinto Callu
 */
var config = require(__dirname+"/config.js");
var enableLogging = process.env.PG_LOG ==='true' ? true : false;



var connection = {};
connection.logQuery = function(startTime, query, values){
  if( ! enableLogging ) return;
  var t = process.hrtime(startTime);
  var valuesQuery = values ? " , values => " + values : "";
  console.log("Query took: %d:%ds  => " + query + valuesQuery,t[0],t[1]);
  console.log();
};
connection.databaseRunning = false;
connection.databaseName = "";
connection.databaseAddr = "";
connection.pg = { end: function(){} };
var cl;
connection.DBClient = require('pg').native;
connection.DBClientSync = require('pg-native');
connection.pgSync = cl;
connection.syncClientLogout = 0;

connection.timeoutLogoutSyncClient = function(){
  connection.syncClientLogout = setInterval(connection.logoutSyncClient,5 * 60e3);
};

connection.logoutSyncClient = function(){
  cl = undefined;
  if( connection.pgSync instanceof Object && typeof connection.pgSync.end === 'function' ){
    connection.pgSync.end();
  }
  clearInterval(connection.syncClientLogout);
};

connection.conString = function(dbName,dbAddr){
  return "postgresql://postgres@" + (dbAddr) + ":" + ( process.env.PG_PORT || 5432 ) + "/" + (dbName) ;
};

connection.querySync = function(query,params,callback){
  var startTime = process.hrtime();
  clearInterval(connection.syncClientLogout);
  if (typeof params == 'function'){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  var ret = { error: undefined, rows: undefined };
  if( !cl ){
    var PGClient = connection.DBClientSync;
    cl = new PGClient();
    try {
      cl.connectSync(connection.conString(connection.databaseName,connection.databaseAddr));
    } catch(e){ ret.error = e; }
  }
  if( typeof ret.error !== 'undefined' ) return ret;
  connection.pgSync = cl;
  try {
    ret.rows = connection.pgSync.querySync(query,params);
  } catch(e) {
    ret.error = e;
  }
  connection.timeoutLogoutSyncClient();
  setImmediate(function(){
    if(enableLogging){ connection.logQuery(startTime, query, params); }
  });
  callback(ret.error, ret.rows);
  return ret;
};

connection.newAsyncPGClient = function(){
  connection.databaseRunning = true;
  if( !connection.pg.connect ) {
    connection.pg = connection.DBClient;
    connection.pg.defaults.poolSize = process.env.PG_POOL_SIZE || 20;
    connection.pg.defaults.parseInt8 = parseInt; // this will fail when int8 are greater than 64-bit.
  }
};

connection.query = function(query, params, callback){
  if( !connection.databaseRunning ) { connection.newAsyncPGClient(); }
  if (typeof params == 'function'){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  var startTime = enableLogging ? process.hrtime() : undefined;
  connection.pg.connect(connection.conString(connection.databaseName,connection.databaseAddr), function(err, client, done) {
    if ( !err ){
      client.query(query, params, function(err, result) {
        done();
        var log = enableLogging ? connection.logQuery(startTime, query, params) : undefined;
        if(err){
          err.message = err.message += "\n "+query;
        }
        callback(err, result);
      });
    } else {
      var killClient;
      if(err) {
        killClient = typeof done === 'function' ? done : function(){};
        killClient();
        connection.pg.end();
        connection.newAsyncPGClient();
        callback(err);
      } else {
        killClient = typeof done === 'function' ? done : function(){};
        killClient();
        callback();
      }
    }
  });
};

connection.end = function(){
  connection.databaseRunning = false;
  try {
    connection.pgSync.end();
  } catch(e){ }
  connection.pg.end();
  clearInterval(connection.syncClientLogout);
};

connection.escapeApostrophes = function (stringToReplace) {
  if(typeof stringToReplace === 'string')
    return stringToReplace.replace(/\'/g, "''");
  return null;
};


module.exports = connection;
