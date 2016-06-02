var config = {}

var DEBUG = process.env.PGJC_DEBUG == 'true';
config.PGJC_POOL_SIZE = process.env.PGJC_POOL_SIZE || process.env.PG_POOL_SIZE || 10;
config.PGJC_NODE_ENV = process.env.ENV || process.env.NODE_ENV || 'development';
config.PGJC_REAP_INTERVAL_MILLIS = process.env.PGJC_REAP_INTERVAL_MILLIS || process.env.REAP_INTERVAL_MILLIS || 1000;
config.PGJC_POOL_IDLE_TIMEOUT = process.env.PGJC_POOL_IDLE_TIMEOUT || process.env.POOL_IDLE_TIMEOUT || 3e4;
config.PGJC_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.PGJC_DB_LOG_SLOW || false;
config.PGJC_DB_LOG = process.env.DB_LOG || process.env.PGJC_DB_LOG || false;
config.PGJC_DB_LOG_CONNECTIONS = process.env.PGJC_DB_LOG_CONNECTIONS || false;
switch(DEBUG){
  case 'debug':
    config.PGJC_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.PGJC_DB_LOG_SLOW || 'true';
    config.PGJC_DB_LOG = process.env.DB_LOG || process.env.PGJC_DB_LOG || 'true'
    config.PGJC_DB_LOG_CONNECTIONS = process.env.PGJC_DB_LOG_CONNECTIONS || 'true';
    break;
  default:
    break;
}
config.LOG_CONNECTIONS = config.PGJC_DB_LOG_CONNECTIONS != 'false';
config.PG_POOL_SIZE = config.PGJC_POOL_SIZE || 10;
config.NODE_ENV = config.PGJC_NODE_ENV == 'true';
config.DB_LOG_ON = config.PGJC_DB_LOG == 'true'
config.DB_LOG_SLOW_QUERIES_ON = config.PGJC_DB_LOG_SLOW == 'true'
config.IS_DEV_ENV =  config.NODE_ENV === 'development'
config.TIMEZONE = process.env.PGJC_TIMEZONE ? process.env.PGJC_TIMEZONE : "America/Los_Angeles"
module.exports = config;
