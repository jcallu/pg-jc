/**
 * @module AbstractTable
 * @author Jacinto Callu
 */
var connection = require(__dirname+'/connection.js');
var _ = require('lodash');
var async = require('async');
var Q = require('q');
var fs = require('fs');
var inflection = require('inflection');


var DUPLICATE_KEY_VIOLATION = '23505';

var CACHE = "_TABLES_SCHEMA_CACHE";

function schemaQuery(dbName){
  return "select table_name as tablename, array_agg(column_name) as schema from (SELECT ist.table_name, isc.column_name::text "+
    "FROM information_schema.tables ist "+
    "inner join information_schema.columns isc on isc.table_name = ist.table_name "+
    "WHERE ist.table_catalog = '"+dbName+"' and ist.table_type = 'BASE TABLE' and ist.table_schema = 'public' "+
    "group by ist.table_name,isc.column_name::text "+
    "ORDER BY ist.table_name::text,isc.column_name::text) s group by 1;";
}

function createVirtualSchema(dbName,dbAddr){
  /*var start = process.hrtime();*/
  var DB = dbName.toUpperCase();
  var schemaTmp;
  if( _.keys(process[DB+CACHE]).length === 0 ) {
    process[DB+CACHE] = process[DB+CACHE] || {};
    connection.databaseName = dbName;
    connection.databaseAddr = dbAddr;
    schemaTmp = connection.querySync(schemaQuery(dbName));
    var pathToFileCache = __dirname+'/schemas/'+DB+CACHE+'.json';
    if( schemaTmp.error || !( schemaTmp instanceof Object ) || ! ( schemaTmp.rows instanceof Array ) || schemaTmp.rows.length === 0  ){
      var schemaFromFile;
      try {
        //abstractDBLog("Using file DB cache");
        schemaFromFile = JSON.parse(fs.readFileSync(pathToFileCache).toString('utf8'));
      } catch(e){
        var err = pathToFileCache + " does not exist";
        if( schemaTmp.error ){
          console.error("Schema Loading error =>",schemaTmp.error);
        }
        throw err;
      }
      schemaTmp = schemaFromFile;
    }
    else {
      //abstractDBLog("Using query DB cache");
      schemaTmp = schemaTmp.rows;
      fs.writeFileSync(pathToFileCache,JSON.stringify(schemaTmp));
    }

    _.each(schemaTmp,function(obj){
      process[DB+CACHE][obj.tablename] = obj.schema;
    });
    connection.logoutSyncClient();
  } else {
    /*abstractDBLog("Using memory DB cache");*/
  }
  /*var t= process.hrtime(start);*/
  /*abstractDBLog("Query took: "+t[0]+":"+t[1].toString().slice(0,2)+"s  => loading schema");*/
}

var AbstractDB = function(tablename,dbName,databaseAddr,client){
  var db = dbName;
  this.db = dbName;
  var DB = dbName.toUpperCase();
  this.DB = DB;
  this.dbName = dbName;
  this.dbAddr = databaseAddr;
  createVirtualSchema(dbName,databaseAddr);
  var schema = process[DB+CACHE][tablename];
  this.schema = schema || [];
  this.tablename = tablename || undefined;
  this.primary_key = this.schema.indexOf(this.tablename+"_id") > -1 ? this.tablename+"_id" : undefined;
  this.reset();
  createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(this.schema);

  this.client = client || undefined;
};

function createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(schema){
  var idColumns = _.compact(_.map(schema,function(colName){
    var endsIn_id =  colName.lastIndexOf('_id') === (colName.length-3) ;
    if( endsIn_id  )
      return { functionId: inflection.camelize(colName), colName: colName };
    return null;
  }));
  if( idColumns.length=== 0 ){
    return;
  }
  _.each(idColumns,function(camelizedColObj){
    //findAllByAnyTableId
    AbstractDB.prototype['findBy'+camelizedColObj.functionId] = function(idIntegerParam){
      this.reset();

      var camelizedColName = camelizedColObj.colName;

      if( parseInt(idIntegerParam).toString() === 'NaN' ) { this.error = new Error('findBy'+camelizedColObj.functionId + " first and only parameter must be a "+camelizedColName+" integer and it was => " + typeof idIntegerParam );
      }

      this.primaryKeyLkup = camelizedColName && camelizedColName === this.primary_key ? true : false;
      if( this.query.trim().indexOf('select') === -1 )
        this.query = "SELECT "+this.tablename+".* FROM "+ this.tablename + " " + this.tablename;
      return this.where(camelizedColName+"="+idIntegerParam);
    };

    //getIds return 1 record if calling getAll<PrimaryKeyId>s without whereParams
    AbstractDB.prototype['getAll'+camelizedColObj.functionId+'s'] = function(whereParams){
      this.reset();
      var camelizedColName = camelizedColObj.colName;
      this.primaryKeyLkup = _.isUndefined(whereParams) && camelizedColName === this.primary_key ? true : false;
      var DISTINCT = !_.isUndefined(whereParams) ? 'DISTINCT' : '';
      if(  this.primaryKeyLkup  ){
        DISTINCT = '';
      }
      this.query = "SELECT "+DISTINCT+" "+this.tablename+"."+camelizedColName+" FROM "+ this.tablename + " " + this.tablename;
      if( !_.isUndefined(whereParams) ){
        this.where(whereParams);
      }
      //console.log("this.query",this.query);
      return this;
    };
  });
}

AbstractDB.prototype.rawsql =  function (rawSql){
  this.reset();
  this.query = rawSql;
  return this;
};

AbstractDB.prototype.select =  function (selectParams){
  this.reset();
  this.selecting = true;
  this.whereQuery = '';
  this.query = '';
  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = selectParams.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if( rawSQLStr.indexOf('select') === 0 )
      this.query = " "+rawSQLStr+" "; // select * from this.tablename expected;
    else if ( rawSQLStr.indexOf('select') !== 0 )
      this.query = "SELECT "+selectParams+" FROM "+this.tablename;
    else
      this.query = updateObjOrRawSQL;
  } else {
    if( ! ( selectParams instanceof Array ) ) selectParams = [];
    var tableName = this.tablename;
    selectParams = _.map(selectParams,function(colName){
      if( colName.indexOf('.') > -1 ) return colName;
      else return tableName + "."+colName;
    });
    if( selectParams.length === 0 ) selectParams = ['*'];
    this.query = "SELECT "+selectParams.join(' , ')+" FROM "+ this.tablename+" "+this.tablename + " ";
  }

  return this;
};

AbstractDB.prototype.selectAll = function(){
  return this.select();
};

AbstractDB.prototype.selectWhere = function(selectWhereParams,whereObjOrRawSQL){
  return this.select(selectWhereParams).where(whereObjOrRawSQL);
};

function externalJoinHelper(obj){
  var onCondition = "TRUE";
  _.forEach(obj,function(value,key){
    onCondition += " AND "+value + " = " + key + " ";
  });
  return onCondition;
}

AbstractDB.prototype.join = function(tablesToJoinOnObjs){
  var self = this;
  var DB = self.DB;
  var rawSql = typeof tablesToJoinOnObjs === 'string' ? tablesToJoinOnObjs : null;
  var joinSQL = '';
  if( rawSql){
    joinSQL = " " + rawSql + " ";
  } else {
    var tables = tablesToJoinOnObjs;
    if( !( tables instanceof Object ) ){
      tables = {};
    }
    var thisTableName = this.tablename;
    _.forEach(tables,function(obj,tablename){
      var schema = process[DB+CACHE][tablename];
      obj.on = obj.on instanceof Array ? obj.on : [];
      var tableName = tablename;
      var alias = obj.as || tablename;
      var onArray = _.compact(_.map(obj.on,function(joinOnColumnsOrObj){
        if( typeof joinOnColumnsOrObj === 'string' && schema.indexOf(joinOnColumnsOrObj) > -1 ){
          return " "+alias+"."+joinOnColumnsOrObj+" = " +thisTableName+"."+joinOnColumnsOrObj+" ";
        }
        if( joinOnColumnsOrObj instanceof Object && _.keys(joinOnColumnsOrObj).length >= 1 ){
          return externalJoinHelper(joinOnColumnsOrObj);
        }
        return null;
      }));
      if( onArray.length === 0 ) onArray = ['false'];
      joinSQL = " "+( obj.type||'INNER' ).toUpperCase() +" "+"JOIN "+ tableName + " " + alias + " ON " + onArray.join(' AND ') + " ";
    });
  }

  this.query += joinSQL;
  return this;
};




AbstractDB.prototype.insert = function(optionalParams){
  this.reset();
  this.inserting = true;
  this.query = "INSERT INTO " + this.tablename + " ";
  if( optionalParams instanceof Object ){
    this.values(optionalParams);
  }
  return this;
};

AbstractDB.prototype.values = function(params){
  var self = this;
  var table_id = self.tablename + "_id";
  var count = 1;
  var schema = self.schema;
  var keys = _.filter(_.keys(params), function(col){
    return schema.indexOf(col) > -1;
  });
  if( keys.length === 0 )  {  this.error = new Error("No insert values passed"); return this; }
  var queryParams = "";
  var columnNames = [];
  var selectValuesAs = [];
  var columnsAndData = _.map(keys, function(key){return params[key]; });
  _.forEach(params, function(value,key){
    try {
      if( !key ) return;
      var ofTypeColumn = '';
      var fieldValue = '';

      if( _.isNull(value) || _.isUndefined(value) ){
        ofTypeColumn = 'null';
      }
      else if( value instanceof Object && value.pgsql_function instanceof Object ){
        var functionToRun = _.keys(value.pgsql_function)[0]  || "THROW_AN_ERROR";
        var pgFunctionInputs = [];
        if (!functionToRun && typeof functionToRun !== String) {
          console.error("functionToRun in Values is not a String or is undefined");
        }
        var values = _.values(value.pgsql_function)[0] || [];
        if (!values && (typeof functionToRun !== Array || values.length === 0)) {
          console.error("values in Values is not an Array or is length of zero");
        }
        pgFunctionInputs = _.map(values,function(val){
          if ( typeof val === 'string' )
            return "'" + connection.escapeApostrophes(val) + "'";
          else
            return val;
        });
        ofTypeColumn = 'pgsql_function';
        value = functionToRun + "("+pgFunctionInputs.join(',')+") ";
      }
      else if( typeof value === 'object' && value instanceof Date ){
        ofTypeColumn = 'date';
        //if( !value ){
        //  ofTypeColumn = 'null';
        //  value = null;
        //}
      }
      else if( ( key.lastIndexOf('_id') === (key.length-3) || key.trim() === 'score' )   && parseInt(value) > 0 )  {
        ofTypeColumn = 'int' ;
      }
      else if( typeof value === 'boolean' ){
        ofTypeColumn = 'bool';
      }
      else if( typeof value === 'string' ){
        ofTypeColumn = 'text';
        value = connection.escapeApostrophes(value);
      } else {
        try {
          value = value.toString();
          value = connection.escapeApostrophes(value);
          ofTypeColumn = 'text';
        } catch(e){
          ofTypeColumn = 'null';
        }
      }
      //abstractDBLog("INSERT VALUES => type="+ofTypeColumn+" , value="+value+", column_name="+key);
      switch(ofTypeColumn){
        case 'date':
          columnNames.push(key);
          fieldValue = value.toISOString();
          selectValuesAs.push(" '"+fieldValue+"'::TIMESTAMP as "+key+" ");
          queryParams += " AND " + key + " IS " + fieldValue + " ";
          break;
        case 'bool':
          columnNames.push(key);
          fieldValue = value ? "TRUE" : "FALSE";
          selectValuesAs.push(" "+fieldValue+" as " + key+" ");
          queryParams += " AND " + key + " IS " + fieldValue + " ";
          break;
        case 'int':
          columnNames.push(key);
          fieldValue = parseInt(value) || null;
          selectValuesAs.push(" "+fieldValue+" as " + key+" ");
          queryParams += " AND " + key + " = " + fieldValue + " ";
          break;
        case 'null':
          columnNames.push(key);
          fieldValue = null;
          selectValuesAs.push(" null as " + key+" ");
          queryParams += " AND " + key + " IS NULL ";
          break;
        case 'text':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" '"+fieldValue + "' as " + key+ " ");
          queryParams += " AND " + key + " = '" + fieldValue + "' ";
          break;
        case 'pgsql_function':
          // actual switch
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" "+fieldValue + " as " + key+ " ");
          queryParams += " AND " + key + " = " + fieldValue + " ";
          break;
        default:
          break;
      }
    } catch(e){
      e.message = e.message+"\nabstractdb::values value => "+ value+", key => "+key;
      console.error(e.stack);
      this.error = e.stack;
    }
  });
  this.query += " (" + columnNames.join(",") + ") SELECT " + selectValuesAs.join(', ') + " ";
  this.whereUniqueParams = queryParams;
  return this;
};

AbstractDB.prototype.unique = function(params){

  var whereUnique = " WHERE NOT EXISTS ( SELECT 1 FROM "+  this.tablename + " WHERE true ";
  whereUnique += this.whereUniqueParams;
  whereUnique += " ) ";
  this.query += whereUnique;

  return this;
};


AbstractDB.prototype.insertUnique = function(params){
  this.insert();
  this.values(params);
  this.unique();
  return this;
};

AbstractDB.prototype.update = function(updateObjOrRawSQL){

  this.reset();
  this.updating = true;
  this.query = '';
  var isRawSQL = typeof updateObjOrRawSQL === 'string' ? true : false;
  if( isRawSQL ){
    this.set(updateObjOrRawSQL);
  }
  this.query  = 'UPDATE '+this.tablename + ' ';

  if( updateObjOrRawSQL instanceof Object ) {
    this.set(updateObjOrRawSQL);
  }

  return this;
};


AbstractDB.prototype.set = function(updateObjOrRawSQL){

  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = updateObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if ( rawSQLStr.indexOf('set') !== 0 )
      this.query += "SET "+updateObjOrRawSQL+" ";
    else
      this.query += updateObjOrRawSQL.trim();
    return this;
  } else {
    var sql = "SET ";
    _.forEach(updateObjOrRawSQL,function(value,key){
      if( _.isNull(value) || _.isUndefined(value) )
        sql +=  key + " = NULL " + " , ";
      else if(  value instanceof Object && typeof value.condition === 'string' )
        sql += key + " = "+ value.condition + " , ";
      else if( typeof value === 'object' && value instanceof Date ){
        sql += key + " = '"+ value.toISOString() + "'::TIMESTAMP , ";
      }
      else if( typeof value === 'boolean' ) {
        sql += key + " = " + value + " , ";
      }
      else if ( ( key.lastIndexOf('_id') === (key.length-3) || key.trim() === 'score' )   && parseInt(value) > 0 ) {
        sql += key + " = "+ parseInt(value) + " , ";
      }
      else {
        try {
          value = value.toString();
        } catch(e) { value = ''; }
        sql += key + " = " + " '"+connection.escapeApostrophes(value)+"' " + " , ";
      }
    });
    sql = sql.slice(0, sql.lastIndexOf(" , "));
    this.query += sql;
    return this;
  }
};


AbstractDB.prototype.deleteFrom = function(){
  this.reset();
  this.deleting = true;
  //console.log("client");
  this.query = "DELETE FROM "+this.tablename+ " WHERE FALSE";
  return this;
};

function generateWhereObj(whereObjOrRawSQL){

  var isRawSQL = typeof whereObjOrRawSQL === 'string' ? true : false;
  var where = null;
  try {
    if( isRawSQL ){
      where = '';
      var rawSQLStr = whereObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ");
      var isNotWhereAddOn = rawSQLStr.indexOf(" where ") === -1;
      if( rawSQLStr.indexOf("where true") >= 0 && isNotWhereAddOn ){
        where += " " + whereObjOrRawSQL + " "; // Syntax Sugar query expected here "WHERE TRUE blah and blah"
        //abstractDBLog("1st str whereParam =>",whereObjOrRawSQL);
      }
      else if ( rawSQLStr.indexOf("and") === -1 && rawSQLStr.indexOf('where') === -1 && rawSQLStr && isNotWhereAddOn ){
        where += " WHERE TRUE AND "+ whereObjOrRawSQL + " "; //Where starts on first condition without "AND" insensitive case
        //abstractDBLog("2nd str  whereParam =>",whereObjOrRawSQL);
      }
      else if ( rawSQLStr.indexOf("and") === 0 && isNotWhereAddOn  ) {
        where += " WHERE TRUE "+whereObjOrRawSQL + " "; //Starts with "AND" insensitive case
        //abstractDBLog("3rd str  whereParam =>",whereObjOrRawSQL);
      }
      else if ( rawSQLStr && isNotWhereAddOn ) {
        where += " WHERE "+whereObjOrRawSQL+ " "; // ANY corner case not handled like passing white space
        //abstractDBLog("4th str  whereParam =>",whereObjOrRawSQL);
      }
      else if ( !isNotWhereAddOn && rawSQLStr.indexOf("and") !== 0 ){
        where += " AND " + whereObjOrRawSQL + " ";
        //abstractDBLog("5th str  whereParam =>",whereObjOrRawSQL);
      }
      else {
        where += " "+whereObjOrRawSQL+" ";
        //abstractDBLog("6th str  whereParam =>",whereObjOrRawSQL);
      }
    }
    else {
      where = '';
      where = " WHERE TRUE ";
      _.forEach(whereObjOrRawSQL, function(value,key){
        if( _.isNull(value) || _.isUndefined(value) )
          where += " AND " + key + " IS NULL ";
        else if( ( key.lastIndexOf('_id') === (key.length-3) || key.trim() === 'score' )   && parseInt(value) > 0  ){
          where += " AND " + key + " = "+parseInt(value)+" ";
        }
        else if(  typeof value === 'object' && value instanceof Date )
          where += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
        else if(  value instanceof Object && value.condition )
          where += " AND " + key + " "+ value.condition + " ";
        else if( key === 'raw_postgresql' ) {
          where += " " + value + " ";
        }
        else if(  typeof value === 'boolean' ){
          where += " AND " + key + " IS "+ value + " ";
        }
        else {
          try {
            value = value.toString();
          } catch(e){
            value = '';
          }
          where += " AND " + key + " = '" + connection.escapeApostrophes(value) + "' ";
        }
      });
    }
  } catch(e){
    var data = '';
    try{ data = JSON.stringify(whereObjOrRawSQL); } catch(e1){ }
    e.message = e.message+"\whereObjOrRawSQL value => "+ data;
    console.error(e.stack);
  }
  return where;
}
AbstractDB.prototype.where = function(whereObjOrRawSQL){

  var selectTmp = this.query.toLowerCase().trim().replace(/(\s{1,})/gm," ");
  if( !selectTmp && selectTmp.indexOf('select') === -1 && selectTmp.indexOf('update '+this.tablename) === -1  && selectTmp.indexOf('delete from') === -1  ) {
    this.query = "SELECT * FROM "+this.tablename + " "+this.tablename+ " ";
  }

  if( ! whereObjOrRawSQL ) this.primaryKeyLkup = false;

  var where = generateWhereObj(whereObjOrRawSQL);


  if( typeof where === 'string' && where.length > 7 && this.deleting ){ // unlocking delete safety
    this.query = this.query.replace("DELETE FROM "+this.tablename+" WHERE FALSE","DELETE FROM "+this.tablename+" ");
  }

  this.whereQuery = where;
  this.query += (where||'');
  this.optimizeQuery();


  return this;
};


AbstractDB.prototype.orderBy = function(arrOrRawOrderBy){

  var isRawSQL = typeof arrOrRawOrderBy === 'string' ? true : false;
  var orderByStr= '';

  if( isRawSQL ) {
    var rawSQLStr = arrOrRawOrderBy.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    orderByStr += arrOrRawOrderBy;
  } else {
    arrOrRawOrderBy = arrOrRawOrderBy instanceof Array ? arrOrRawOrderBy : [];
    _.each(arrOrRawOrderBy,function(ele){
      if ( typeof ele === 'string' ){
        orderByStr += " "+ele+" asc";
      } else if( ele instanceof Object ){
        orderByStr += " "+ele+" asc";
      }
    });
    if( arrOrRawOrderBy.length === 0 )
      orderByStr += " 1 ";
  }

  this.query += " ORDER BY ";
  orderByStr = orderByStr ? orderByStr : '1'; // default order by first param;
  this.query += " " + orderByStr + " ";
  return this;
};

/*
 *                                                     // optional params          //optional param
 *  @usage .AndNotExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'})
 */
AbstractDB.prototype.AndNotExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL){
  return this.AndExists(tableNameExists,onColumnIds,whereExistsObjOrSQL,false);
}

/*
 *                                                     // optional params          //optional param
 *  @usage .AndExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'},true)
 */
AbstractDB.prototype.AndExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL,NOT){
  NOT = typeof NOT === 'boolean' && !NOT ? " NOT " : "";
  onColumnIds = _.isNull(onColumnIds) || _.isNull(onColumnIds) ? [] : onColumnIds;
  onColumnIds = onColumnIds instanceof Array ? onColumnIds : [onColumnIds];

  if( typeof whereExistsObjOrSQL === 'boolean' ) {
    NOT = whereExistsObjOrSQL ? '' : ' NOT ';
    whereExistsObjOrSQL = null;

  }

  var whereQuery = whereExistsObjOrSQL ? generateWhereObj(whereExistsObjOrSQL) : ' WHERE TRUE ';
  var mainTableName = this.tablename;
  var whereOnColumnIdsAnd = onColumnIds.length > 0 ? " AND " : " ";
  whereQuery =  "AND "+NOT+" EXISTS (select 1 from "+tableNameExists+ " "+tableNameExists+" "+
                whereQuery+whereOnColumnIdsAnd+
                _.map(onColumnIds,function(colName){
                  return " "+tableNameExists+"."+colName+" = "+mainTableName+"."+colName + " ";
                }).join(" AND ") +
                " )";
  this.whereQuery += " "+whereQuery+" ";
  this.query += " "+whereQuery+" ";
  return this;
};

AbstractDB.prototype.groupBy = function(textOrObj){
  var isRawSQL = typeof textOrObj === 'string' ? true : false;
  var orderByStr = '';
  if( isRawSQL ){
    var rawSQLStr = textOrObj.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    orderByStr += "GROUP BY "+textOrObj;
  }
  else if ( textOrObj instanceof Array ){
    orderByStr += "GROUP BY "+textOrObj.join(', ');
  }
  else {}
  this.query += " " + orderByStr + " ";
  return this;
};

AbstractDB.prototype.having = function(i){
  this.query += " HAVING "+i+" ";
  return this;
};

AbstractDB.prototype.offset = function(i){

  this.query += " OFFSET "+(parseInt(i)||0)+" ";
  return this;
};


AbstractDB.prototype.limit = function(i){

  this.query += " LIMIT "+(parseInt(i)||'ALL')+" ";
  return this;
};


AbstractDB.prototype.optimizeQuery = function(){
  if( this.primaryKeyLkup ){
    this.limit(1);
  }
  return this;
};

AbstractDB.prototype.dbQuery = function(query,callback){
  var self = this;
  connection.databaseName = self.dbName;
  connection.databaseAddr = self.dbAddr;

  var queryCallback = function(err,results){
      if(err) return callback(err,null);
      result = results instanceof Object && results.rows instanceof Array ? results.rows : [];
      callback(null,result);
  };

  if( _.isUndefined(this.client) ){
    connection.query(query,queryCallback);
  } else {
    this.client.query(query,[],queryCallback);
  }
};

AbstractDB.prototype.dbQuerySync = function(query){
  var self = this;
  connection.databaseName = self.dbName;
  connection.databaseAddr = self.dbAddr;
  var ret = connection.querySync(query);
  ret.failed = ret.error ? true : false;
  ret.Rows = function () {  return ret.rows; };
  ret.Error = function () {  return ret.error; };
  return ret;
};

AbstractDB.prototype.run = function(callback){
  this.finalizeQuery();

  var Query = this.query + this.returnIds;
  var IS_PROMISED = typeof callback !== 'function';
  var q;
  if( IS_PROMISED ) q = Q.defer();
  var self = this;

  callback = typeof callback === 'function' ? callback : function(){};
  if( self.error ){
    if(IS_PROMISED ) q.reject(self.error);
    else callback(self.error,null);
    self.reset();
  } else {
    self.dbQuery(Query,function(err,rows){
      if(err ) {
        //console.error("Error query =>",Query);
        if(IS_PROMISED ) q.reject(err);
        else callback(err,null);
      }
      else {
        if(IS_PROMISED ) q.resolve(rows);
        else callback(null,rows);
      }
      self.reset();
    });
  }
  if( IS_PROMISED) return q.promise;
};




AbstractDB.prototype.runSync = function(callback){

  this.finalizeQuery();
  var Query = this.query + this.returnIds;
  callback = typeof callback === 'function' ? callback : function(){};

  var self = this;
  var ret = self.dbQuerySync(Query);
  self.Rows = ret.Rows;
  self.Error = ret.Error;
  self.results = { error: self.Error(), rows: self.Rows() };
  return self;
};

AbstractDB.prototype.finalizeQuery = function(){
  var query = this.query.toLowerCase().trim().replace(/\W/gm,"").trim();
  //console.log("query",query);
  if ( query.indexOf("insertinto") === 0  || query.indexOf("update"+this.tablename+"set") === 0 || query.indexOf("deletefrom") ===0 ) {
    if ( this.query.indexOf("RETURNING " + this.tablename + "_id") === -1 && this.schema.indexOf(this.tablename + "_id") > -1 ) {
      this.returnIds = " RETURNING " + this.tablename + "_id ";
    } else {
      this.returnIds = " RETURNING * ";
    }
  }
  return this;
};

AbstractDB.prototype.printQuery = function(ovrLog){
  //abstractDBLog("this =>",this);
  this.finalizeQuery();
  var Query = this.query + this.returnIds;
  var queryLog = "\nquery => " + Query + "\n";
  if( ! ovrLog ){
    abstractDBLog(queryLog);
    return this;
  }
  console.log(queryLog);
  return this;
};

AbstractDB.prototype.reset = function(callback){
  callback = typeof callback === 'function' ? callback : function(){};
  this.query = '';
  this.primaryKeyLkup = false;
  this.whereQuery = null;
  this.deleting = false;
  this.inserting = false;
  this.updating = false;
  this.returnIds = '';
  this.upserting = false;
  this.utilReady = false;
  this.error = null;
  callback();
};


AbstractDB.prototype.util = function(){
  this.reset();
  this.utilReady = true;
  return this;
};

AbstractDB.prototype.upsert = function(setParams,whereParams,callback){
  if( !this.utilReady ) return callback(new Error("Need to call util() before accessing utility functions"));
  if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) return callback(new Error("Can only insert or update object params"));
  var self = this;
  var tableNameId = self.schema.indexOf(self.tablename+"_id") > -1 ? self.tablename+"_id" : "*";
  var ret = [];
  async.series([
    function update(scb){
      self.update()
      .set(setParams)
      .where(whereParams)
      .run(function(err,results){
        //if(err) { console.error("update in upsert",err); }
        if( results instanceof Array && results.length > 0 ) {
          ret = results;
        }
        scb();
      });
    },
    function insert(scb){
      if( ret.length > 0 ) return scb();
      self.insert()
      .values(setParams)
      .run(function(err,results){
        var IS_DUPLICATE_KEY_VIOLATION = err instanceof Object && err.sqlState === DUPLICATE_KEY_VIOLATION;
        if( results instanceof Array && results.length > 0 )
          ret = results;
        if( ! IS_DUPLICATE_KEY_VIOLATION )
          return scb(err);
        scb();
      });
    },function select(scb){
      if( ret.length > 0 ) return scb();
      self.select([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    callback(err||null,ret);
  });
};


AbstractDB.prototype.upsertUsingColumnValues = function(setParams,whereParams,callback){
  if( !this.utilReady ) return callback(new Error("Need to call util() before accessing utility functions"));
  if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) return callback(new Error("Can only insert or update object params"));
  var self = this;
  var tableNameId = self.schema.indexOf(self.tablename+"_id") > -1 ? self.tablename+"_id" : "*";
  var ret = [];
  async.series([
    function update(scb){
      self.update()
      .set(setParams)
      .where(whereParams)
      .run(function(err,results){
        //if(err) { console.error("update in upsert",err); }
        if( results instanceof Array && results.length > 0 ) {
          ret = results;
        }
        scb();
      });
    },
    function insert(scb){
      if( ret.length > 0 ) return scb();
      self.insert()
      .values(_.merge(setParams,whereParams))
      .run(function(err,results){
        var IS_DUPLICATE_KEY_VIOLATION = err instanceof Object && err.sqlState === DUPLICATE_KEY_VIOLATION;
        if( results instanceof Array && results.length > 0 )
          ret = results;
        if( ! IS_DUPLICATE_KEY_VIOLATION )
          return scb(err);
        scb();
      });
    },function select(scb){
      if( ret.length > 0 ) return scb();
      self.select([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    callback(err||null,ret);
  });
};





module.exports = AbstractDB;
module.exports.connection = connection;
module.exports.end = connection.end;
module.exports.query = connection.query;
module.exports.escapeApostrophes = connection.escapeApostrophes;
module.exports.querySync = connection.querySync;
module.exports.createVirtualSchema = createVirtualSchema;
module.exports.promise = function(){ var q = Q.defer(); q.resolve(undefined); return q.promise; };
