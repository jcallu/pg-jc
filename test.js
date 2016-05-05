var pgjc = require('./index.js');
var dbName = process.env.PGDATABASE || 'postgres'
var dbUser = process.env.PGUSER || 'postgres'
var dbPasswd = process.env.PGPASSWORD || ''
var dbPort = process.env.PGPORT || 5432;
var dbHost = process.env.PGHOST || '127.0.0.1'
var connStr = "postgresql://"+dbUser+":"+dbPasswd+"@"+dbHost+":"+dbPort+"/"+dbName ;
var vgpdb = pgjc();
console.log(dbName,vgpdb);
