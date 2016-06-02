/**
 * Provides a postgres access point for other functions
 * @module ConnectionClient
 * @author Jacinto Callu
 * @copyright Jacinto Callu 2016
 */
var Q = require('q');
var _ = require('lodash');

/* TheVGP Modules */

var config = require('./config.js');
var moment = require('moment-timezone')
/** Constants **/
var PG_POOL_SIZE = config.PG_POOL_SIZE;
var NODE_ENV = config.ENV;
var DB_LOG_ON = config.DB_LOG;
var DB_LOG_SLOW_QUERIES_ON = process.env.DB_LOG_SLOW ? true : false
var IS_DEV_ENV =  NODE_ENV === 'development'

var pgData = {}
var defaults = {
  reapIntervalMillis: config.PGJC_REAP_INTERVAL_MILLIS || 1000,
  poolIdleTimeout: config.PGJC_POOL_IDLE_TIMEOUT ||  3e4,
  poolSize: config.PG_POOL_SIZE,
  parseInt8: parseInt,
  DB_CONNECTION_ID: pgData.DB_CONNECTION_ID>=0 ? pgData.DB_CONNECTION_ID : 1
}

function TransactionDBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,pgClient){
  this.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser);

  this.pgClientDefaults = _.cloneDeep(defaults);
  this.pgClient = pgClient;
  this.pgClient.defaults = this.pgClientDefaults;
  this.clientConnectionID = 1
}


TransactionDBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.setDatabaseName(databaseName);
  this.setDatabaseAddress(databaseAddress);
  this.setDatabasePort(databasePort);
  this.setDatabaseUser(databaseUser);
  this.setDatabasePassword(databasePassword);
}

TransactionDBConnection.prototype.setDatabaseName = function(dbName){
  this.databaseName = dbName || ''
}

TransactionDBConnection.prototype.setDatabaseAddress = function(dbAddr){
  this.databaseAddress = dbAddr || ''
}

TransactionDBConnection.prototype.setDatabasePort = function(dbPort){
  this.databasePort = dbPort || 5432;
}

TransactionDBConnection.prototype.setDatabaseUser = function(dbUser){
  this.databaseUser = dbUser || 'postgres';
}

TransactionDBConnection.prototype.setDatabasePassword = function(dbPasswd){
  this.databasePassword = dbPasswd || '';
}

/** Setup a new Asynchronous PG Client **/
TransactionDBConnection.prototype.PGNewClientAsync = function(){
  if( !this.databaseName ){    console.error( new Error( "TransactionDBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "TransactionDBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  console.log(this.databaseName,"PG Client Async Size = " + this.pgClient.defaults.poolSize + " :  DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Generate and return a connection string using database name and address **/
TransactionDBConnection.prototype.getConnectionString = function(){
  var user = this.databaseUser;
  var password = this.databasePassword;
  var address = this.databaseAddress;
  var port = this.databasePort;
  var name = this.databaseName;
  var connectionString = "postgresql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ; /* Create a TCP postgresql Call string using a database, password, port, and address; password and port are defaulted currently to config's */
  return connectionString;
};


var async = require('async')
/** Query using the Asynchronous PG Client **/
TransactionDBConnection.prototype.query = function(queryIn, paramsIn, callback){
  var query = _.cloneDeep(queryIn);
  var params = paramsIn instanceof Array ? _.cloneDeep(paramsIn) : paramsIn;

  var startTime = process.hrtime();

  var dbConnectionString = this.getConnectionString();


  if ( typeof params == 'function' ){
    callback = params;
    params = null;
  }
  callback = typeof callback === 'function' ? callback : function(){};

  // console.log("callback",callback)

  var self = this;
  async.waterfall([
      function ifNotConnectedConnect(wcb){
        var isConnected = false
        try { isConnected = self.pgClient.native.pq.connected == true } catch(e){}
        if( isConnected ) return wcb();
        self.pgClient.connect.bind(self.pgClient)(wcb)
      },
      function queryCall(wcb){
        self.pgClient.query.bind(self.pgClient)(query, params, function(err, result) {
          // console.log("err, result",err, result)
          if(err){ try{  err.message = err.message + "\r" + query;  } catch(e){ console.error(e.stack) } }
          self.logQuery.bind(self)(startTime, query, params)
          callback(err, result);
        });
      }
  ],callback)
};


TransactionDBConnection.prototype.querySync = function(queryIn, paramsIn, callback){
  var err = new Error("transaction querySync not supported")
  // callback(err)
  throw err
}


/** Wrapper to end database connection **/
TransactionDBConnection.prototype.end = function(){
  try { this.pgClient.end(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of async PG clients

}

/** Force Sync Clients to die after certain time **/
TransactionDBConnection.prototype.timeoutLogoutSyncClient = function(){

}

/** Force Sync Client to die **/
TransactionDBConnection.prototype.logoutSyncClient = function(){
  try {  } catch(e){  } /* If PGSync Client is alive and well destory it. Feel the power of the darkside!!! */

}

TransactionDBConnection.prototype.logoutAsyncClient = function(){
  try { this.pgClient.end(); } catch(e){  } /* If PGSync Client is kill */
}

/** Async Client has died **/
TransactionDBConnection.prototype.dbClientError = function(err,done,callback){
  done = typeof done === 'function' ? done : function(){}; // make sure client can be killed without any syntax errors.
  callback = typeof callback == 'function' ? callback : function(){};
  err = err ? ( err instanceof Error ? err : new Error(err) ) : new Error(); // make sure error is an Error instance.
  return callback(err);
}

/** Helper Function to log timing of query functions **/
function logQueryPrint(message,query,valuesQuery,seconds,milliseconds){
  console.log(message + "Query took: %d:%ds  => " + query + valuesQuery, seconds, milliseconds); console.log();
}

var monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
var momentRegion = config.TIMEZONE;
function getTimestamp(){
  var date = new Date();
  var dateS = date.toISOString();
  var momentO = moment(dateS).tz(momentRegion);
  var momentS = momentO.format('DD, YYYY hh:mm:ss');
  var dateM = new Date(momentO.format());
  return monthNames[dateM.getMonth()] + " " + momentS + " (PST)"
}


function logQuery(startTime, query, values){
  if( !query ) return console.log("No query passed in",query,values)
  var clientConnectionID = this.clientConnectionID >=0 ? this.clientConnectionID : 'null';
  if( ! ( DB_LOG_ON || DB_LOG_SLOW_QUERIES_ON   ) ) return;
  var t = process.hrtime(startTime);
  var valuesQuery = values instanceof Array && values.length > 0 ? (" , queryParams => [" + values + "]" ) : "";
  var seconds = t[0];
  var milliseconds = t[1];
  var isSlowTiming = ( seconds + (milliseconds/1e9) ) >= 1;
  var message = "Connection "+this.databaseAddress+" ID: "+clientConnectionID+" - "+getTimestamp()+" - "
  if (  ( DB_LOG_SLOW_QUERIES_ON ||  IS_DEV_ENV ) && isSlowTiming  ){
    message = "Connection "+this.databaseAddress+" ID: "+clientConnectionID+" SLOW!! - "+getTimestamp()+" - "
    return logQueryPrint(message,query,valuesQuery,seconds,milliseconds)
  }
  if ( DB_LOG_ON ){ return logQueryPrint(message,query,valuesQuery,seconds,milliseconds) }
};

TransactionDBConnection.prototype.logQuery = logQuery

module.exports = TransactionDBConnection;
