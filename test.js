var Db = require('./index.js');
var dbName = process.env.DB_NAME || 'postgres'
var connStr = "postgresql://postgres@127.0.0.1:5432/"+dbName ;
var db = Db(connStr);
console.log(dbName,db);
