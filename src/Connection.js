

var Q = require('q');
var _ = require('lodash');
var moment = require('moment-timezone')
var d = new Date(); // or whatever date you have
var tzName = d.toLocaleString('en', {timeZoneName:'short'}).split(' ').pop();
/** Constants **/
var DEBUG = process.env.PGJC_DEBUG == 'true';
process.env.PGJC_POOL_SIZE = process.env.PGJC_POOL_SIZE || process.env.PG_POOL_SIZE || 10;
process.env.PGJC_NODE_ENV = process.env.ENV || process.env.NODE_ENV || 'development';
process.env.PGJC_REAP_INTERVAL_MILLIS = process.env.PGJC_REAP_INTERVAL_MILLIS || process.env.REAP_INTERVAL_MILLIS || 1000;
process.env.PGJC_POOL_IDLE_TIMEOUT = process.env.PGJC_POOL_IDLE_TIMEOUT || process.env.POOL_IDLE_TIMEOUT || 3e4;
process.env.PGJC_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.PGJC_DB_LOG_SLOW || false;
process.env.PGJC_DB_LOG = process.env.DB_LOG || process.env.PGJC_DB_LOG || false;
process.env.PGJC_DB_LOG_CONNECTIONS = process.env.PGJC_DB_LOG_CONNECTIONS || false;
switch(DEBUG){
  case 'debug':
    process.env.PGJC_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.PGJC_DB_LOG_SLOW || 'true';
    process.env.PGJC_DB_LOG = process.env.DB_LOG || process.env.PGJC_DB_LOG || 'true'
    process.env.PGJC_DB_LOG_CONNECTIONS = process.env.PGJC_DB_LOG_CONNECTIONS || 'true';
    break;
  default:
    break;
}
var LOG_CONNECTIONS = process.env.PGJC_DB_LOG_CONNECTIONS == 'true';
var PG_POOL_SIZE = process.env.PGJC_POOL_SIZE;
var NODE_ENV = process.env.PGJC_NODE_ENV == 'true';
var DB_LOG_ON = process.env.PGJC_DB_LOG == 'true'
var DB_LOG_SLOW_QUERIES_ON = process.env.PGJC_DB_LOG_SLOW == 'true'
var IS_DEV_ENV =  NODE_ENV === 'development'

var PGNativeAsync = require('pg').native
var PGNativeSync = require('pg-native');
var pg = {};//PGNativeAsync;
var pgSync = {};//new PGNativeSync();

var defaults = {
  reapIntervalMillis: process.env.PGJC_REAP_INTERVAL_MILLIS || 1000,
  poolIdleTimeout: process.env.PGJC_POOL_IDLE_TIMEOUT ||  3e4,
  poolSize: PG_POOL_SIZE,
  parseInt8: parseInt,
  DB_CONNECTION_ID: pg.DB_CONNECTION_ID>=0 ? pg.DB_CONNECTION_ID : 1
}

var SYNC_LOGOUT_TIMEOUT = defaults.poolIdleTimeout;

function newPGClientAsync(){
  pg = PGNativeAsync;
  pg.defaults.reapIntervalMillis = defaults.reapIntervalMillis; // check to kill every 5 seconds
  pg.defaults.poolIdleTimeout = defaults.poolIdleTimeout; // die after 1 minute
  pg.defaults.poolSize = defaults.poolSize;
  pg.defaults.parseInt8 = defaults.parseInt8;
  pg.DB_CONNECTION_ID = defaults.DB_CONNECTION_ID;
  if( pg.listeners('error').length === 0 ){
    pg.on('error',function(e){
      console.error("FAILURE - pg module crashed ",e.stack)
    })
  }
  return pg;
}
function newPGClientSync(){
  pgSync = new PGNativeSync();
  pgSync.defaults = !( pgSync.defaults instanceof Object ) ? _.cloneDeep(defaults) : pgSync.defaults;
  pgSync.defaults.poolSize = 1; //sync clients use single connection then die
  return pgSync
}
function isPGClientAsyncDisconnected(conString){
  // try { console.log("pg.pools",pg.pools) } catch(e){}
  // try { console.log("pg.pools.all",pg.pools.all) } catch(e){}
  // try { console.log("pg.pools.all['\"'+conString+'\"']",pg.pools.all['"'+conString+'"']) } catch(e){}
  return !( pg.pools instanceof Object ) || !( pg.pools.all instanceof Object ) || typeof pg.pools.all['"'+conString+'"'] === 'undefined'
}
function isPGClientSyncDisconnected(){
  return typeof pgSync == 'undefined' || !( pgSync.pq instanceof Object) || ( typeof pgSync.pq.connected == 'boolean' && !pgSync.pq.connected )
}


function DBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser);
  this.pgClientSyncIntervalTimer = 0;
  this.pgClientDefaults = _.cloneDeep(defaults);
  this.clientConnectionID = ( parseInt(pg.DB_CONNECTION_ID) >= 1 ? parseInt(pg.DB_CONNECTION_ID) : 1 );
}


DBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.setDatabaseName(databaseName);
  this.setDatabaseAddress(databaseAddress);
  this.setDatabasePort(databasePort);
  this.setDatabaseUser(databaseUser);
  this.setDatabasePassword(databasePassword);
}

DBConnection.prototype.setDatabaseName = function(dbName){
  this.databaseName = dbName || ''
}

DBConnection.prototype.setDatabaseAddress = function(dbAddr){
  this.databaseAddress = dbAddr || ''
}

DBConnection.prototype.setDatabasePort = function(dbPort){
  this.databasePort = dbPort || 5432;
}

DBConnection.prototype.setDatabaseUser = function(dbUser){
  this.databaseUser = dbUser || 'postgres';
}

DBConnection.prototype.setDatabasePassword = function(dbPasswd){
  this.databasePassword = dbPasswd || '';
}

/** Setup a new Asynchronous PG Client **/
DBConnection.prototype.PGNewClientAsync = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }

  var dbConnectionString = this.getConnectionString();
  if( isPGClientAsyncDisconnected(dbConnectionString) ){
    this.logoutAsyncClient()
    newPGClientAsync()
  }
  if( LOG_CONNECTIONS ) console.log(this.databaseName,"PG Client Async Size = " + pg.defaults.poolSize + " :  DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Generate and return a connection string using database name and address **/
DBConnection.prototype.getConnectionString = function(){
  var user = this.databaseUser;
  var password = this.databasePassword;
  var address = this.databaseAddress;
  var port = this.databasePort;
  var name = this.databaseName;
  var connectionString = "postgresql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ;
  return connectionString;
};



/** Query using the Asynchronous PG Client **/
DBConnection.prototype.query = function(query, params, callback){


  var startTime = process.hrtime();

  var dbConnectionString = this.getConnectionString();
  if( isPGClientAsyncDisconnected(dbConnectionString) ) { /* If PG Async Client is disconnected, connect that awesome, piece of awesomeness!!! */
    this.PGNewClientAsync();
  }
  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  pg.connect( dbConnectionString , function(err, client, done) {
    if ( !err  && client instanceof Object  && typeof client.query === 'function' && typeof done === 'function' ) { /*  Check if client connected then run the query */
      client.query(query, params, function(err, result) {
        done();
        self.logQuery(startTime, query, params)
        if(err){  err.message = err.message + "\r" + query;  }
        callback(err, result);
      });
      return;
    }
    self.dbClientError.bind(self)(err, done, callback); /*  The Client died, didn't connect, or errored out; so make sure it gets buried properly, and resurrected immediately for further querying. */
  });

};

/** Setup a new Synchronous PG Client **/
DBConnection.prototype.PGNewClientSync = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  if( isPGClientSyncDisconnected() ){
    this.logoutSyncClient()
    newPGClientSync();
  }
  if( LOG_CONNECTIONS )   console.log(this.databaseName,"PG Client Sync Size = "+pgSync.defaults.poolSize+" :  DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Query using the Synchronous PG Client **
 * WARNING PADWAN!!! : This is for object/array/string initialization using data from the database only.
 * Never use this for regular querying because it will starve CPU for the rest of application
 */
DBConnection.prototype.querySync = function(query,params,callback){
  clearInterval( this.pgClientSyncIntervalTimer );   /* Prevent logout if it has not happened yet. */
  var error = null;
  if( isPGClientSyncDisconnected() ){
    try { this.PGNewClientSync(); } catch(e) { error = e }
  }

  var startTime = process.hrtime();
  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};

  var ret = { error: undefined, rows: [] };

  if( error ) {
    ret.error = error;
    callback(error,ret)
    return ret;
  }


  var dbConnectionString = ''
  try {
    dbConnectionString = this.getConnectionString();
    pgSync.connectSync( dbConnectionString );
    ret.rows = pgSync.querySync( query, params );
  } catch(e) {
    ret.error = e;
    ret.rows = []
  } // run blocking/synchronous query to db, careful because it throws errors so we try/catched dem' bugs
  this.timeoutLogoutSyncClient();
  this.logQuery(startTime, query, params);
  callback(ret.error, ret);
  return ret;
}

/** Wrapper to end database connection **/
DBConnection.prototype.end = function(){
  // console.log("pg",pg,new Error('db.end').stack)
  try { pgSync.end(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of  sync PG clients
  try { pg.end(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of async PG clients
  clearInterval( this.pgClientSyncIntervalTimer );
}

/** Force Sync Clients to die after certain time **/
DBConnection.prototype.timeoutLogoutSyncClient = function(){
  clearInterval( this.pgClientSyncIntervalTimer ); // Just killed PG Sync Client.
  this.pgClientSyncIntervalTimer = setInterval( this.logoutSyncClient.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.pgClientSyncIntervalTimer.unref.bind(this)()
}

/** Force Sync Client to die **/
DBConnection.prototype.logoutSyncClient = function(){
  try { pgSync.end(); } catch(e){  } /* If PGSync Client is alive and well destory it. Feel the power of the darkside!!! */
  clearInterval( this.pgClientSyncIntervalTimer ); // Just killed PG Sync Client.
}

DBConnection.prototype.logoutAsyncClient = function(){
  try { pg.end(); } catch(e){  } /* If PGSync Client is kill */
}

/** Async Client has died **/
DBConnection.prototype.dbClientError = function(err,done,callback){
  done = typeof done === 'function' ? done : function(){}; // make sure client can be killed without any syntax errors.
  callback = typeof callback == 'function' ? callback : function(){};
  err = err ? ( err instanceof Error ? err : new Error(err) ) : new Error(); // make sure error is an Error instance.
  if(err) {
    // console.error("DBConnection.dbClientError",err.stack);
    try {
      pg.end();
    } catch(e){
      // console.error(e.stack)
    };
  } // kill the async client and reload the connection
  try {
    done();
  } catch(e){
    // console.error(e.stack)
  }
  return callback(err);
}

/** Helper Function to log timing of query functions **/
function logQueryPrint(message,query,valuesQuery,seconds,milliseconds){
  console.log(message + "Query took: %d:%ds  => " + query + valuesQuery, seconds, milliseconds); console.log();
}

var monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
var momentRegion = tzName;
function getTimestamp(){
  var date = new Date();
  var dateS = date.toISOString();
  var momentO = moment(dateS).tz(momentRegion);
  var momentS = momentO.format('DD, YYYY hh:mm:ss');
  var dateM = new Date(momentO.format());
  return monthNames[dateM.getMonth()] + " " + momentS + " (PST)"
}


function logQuery(startTime, query, values){
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

DBConnection.prototype.logQuery = logQuery

module.exports = DBConnection;
