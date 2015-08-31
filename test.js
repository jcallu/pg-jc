var db = require('./index.js');
var connStr = "postgresql://postgres@127.0.0.1:5432/vgp";
var vgpdb = db(connStr);


console.log("vgpdb",vgpdb);
