npm install pg-jc



usage:

var pgjc = require('pg-jc')
var pgConnStr = "postgresql://postgres@127.0.0.1:5432/mydb";
var mydb = pgjc(pgConnStr);



mydb.users.selectAll().run(function(err,data){
  if(err) throw err;
  console.log(data);
});
