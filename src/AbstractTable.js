var paths = {}
var PG_JC_HOME = __dirname
var _ = require('lodash');
var async = require('async');
var Q = require('q');
var fs = require('fs');
var NODE_ENV = process.env.NODE_ENV || 'development';
var DB_LOG = process.env.PG_JC_LOG == "true" || process.env.DB_LOG == 'true';
var utilityFunctions = {
  console: { asyncLog: function(){
      var args = _.values(arguments);
      setTimeout(function(){
        console.log.apply(this,args)
      },22)
    }
  }, escapeApostrophes: function(str){
    if( typeof str != 'string' ){
      throw "not a string"
    }
    return str.replace(/\'/gm,"''")
  }
}
var abstractDBLog = NODE_ENV !== 'production' && DB_LOG ? utilityFunctions.console.asyncLog : function(){};


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
function initErrorHandler(e){
  if(e){
    console.error(e.stack)
  }
}
function isCacheNotSet(cacheKey){
  if( typeof process[cacheKey] == 'undefined' ) return true;
  return _.keys( ( process[cacheKey] || {} ) ).length === 0;
}


function createVirtualSchema(dbName,dbAddr,dbPasswd,dbPort,dbUser,dbConnection){
  // console.log("dbName,dbAddr",dbName,dbAddr)
  // var start = process.hrtime();
  var DB = dbName.toUpperCase();
  var CACHE_KEY = DB+CACHE;
  var schemaTmp;
  // console.log("process[CACHE_KEY]",process[CACHE_KEY])
  var setCache = isCacheNotSet(CACHE_KEY)
  // console.log("setCache",setCache)
  if( setCache ) {
    process[CACHE_KEY] = process[CACHE_KEY] || {};
    var data = dbConnection.querySync("select 1 first_db_call_test, '"+dbConnection.databaseAddress+"' as address,'"+dbConnection.databaseName+"' as database")
    initErrorHandler(data.error)
    schemaTmp = dbConnection.querySync(schemaQuery(dbName));
    // dbConnection.query("select 1");
    // console.log("schemaTmp",schemaTmp)
    var pathToFileCache = PG_JC_HOME+'/schemas/'+CACHE_KEY+'.json';
    if( schemaTmp.error || !( schemaTmp instanceof Object ) || ! ( schemaTmp.rows instanceof Array ) || schemaTmp.rows.length === 0  ){
      var schemaFromFile;
      try {
        //abstractDBLog("Using file DB cache");
        schemaFromFile = JSON.parse(fs.readFileSync(pathToFileCache).toString('utf8'));
      } catch(e){

        var err = pathToFileCache + " AbstractTable readFileSync\n"+e.stack;

        if( err || schemaTmp.error ){
          if( schemaTmp.error ) console.error("Schema Loading error =>",schemaTmp.error);
          if ( err ) console.error("Schema Loading error =>",err);
        }
        fs.writeFileSync(pathToFileCache,JSON.stringify({}))
        if( NODE_ENV === 'development' ) {
          throw err;
        }
      }
      schemaTmp = schemaFromFile;
    }
    else {
      //abstractDBLog("Using query DB cache");
      schemaTmp = schemaTmp.rows;
      fs.writeFileSync(pathToFileCache,JSON.stringify(schemaTmp));
    }

    _.each(schemaTmp,function(obj){
      process[CACHE_KEY][obj.tablename] = obj.schema;
    });
    // dbConnection.logoutSyncClient();
  } else {
    // abstractDBLog("Using memory DB cache");
  }
  // var t= process.hrtime(start);
  // abstractDBLog("Query took: "+t[0]+":"+t[1].toString().slice(0,2)+"s  => loading schema");
}

var GenerateWhereObj = function (whereObjOrRawSQL,AND){

  var isRawSQL = typeof whereObjOrRawSQL === 'string' ? true : false;
  var where_CLAUSE = '';
  try {
    if( isRawSQL ){
     where_CLAUSE = '';
      var rawSQLStr = whereObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ").trim();
      // console.log("rawSQLStr ->",rawSQLStr)
      var isNotWhereAddOn = rawSQLStr.indexOf("WHERE TRUE") === -1;
      if ( AND ){
        if( rawSQLStr.indexOf("and") !== 0 ){
          where_CLAUSE += " AND " + whereObjOrRawSQL + " ";
        } else {
          where_CLAUSE += " " + whereObjOrRawSQL + " ";
        }
      } else {
        if(  rawSQLStr.indexOf("where true") >= 0 && isNotWhereAddOn ){
          where_CLAUSE += " " + whereObjOrRawSQL + " "; // Syntax Sugar query expected here "WHERE TRUE blah and blah"
          //abstractDBLog("1st str whereParam =>",whereObjOrRawSQL);
        }
        else if (  rawSQLStr.indexOf("and") === -1 && rawSQLStr.indexOf('where') === -1 && rawSQLStr && isNotWhereAddOn ){
         where_CLAUSE += " WHERE TRUE AND "+ whereObjOrRawSQL + " "; //Where starts on first condition without "AND" insensitive case
          //abstractDBLog("2nd str  whereParam =>",whereObjOrRawSQL);
        }
        else if ( rawSQLStr.indexOf("and") === 0 && isNotWhereAddOn  ) {
         where_CLAUSE += " WHERE TRUE "+whereObjOrRawSQL + " "; //Starts with "AND" insensitive case
          //abstractDBLog("3rd str  whereParam =>",whereObjOrRawSQL);
        }
        else if (  rawSQLStr && isNotWhereAddOn ) {
         where_CLAUSE += " WHERE "+whereObjOrRawSQL+ " "; // ANY corner case not handled like passing white space
          //abstractDBLog("4th str  whereParam =>",whereObjOrRawSQL);
        }
        else if ( !isNotWhereAddOn && rawSQLStr.indexOf("and") !== 0 ){
         where_CLAUSE += " AND " + whereObjOrRawSQL + " ";
          //abstractDBLog("5th str  whereParam =>",whereObjOrRawSQL);
        }
        else {
         where_CLAUSE += " "+whereObjOrRawSQL+" ";
          //abstractDBLog("6th str  whereParam =>",whereObjOrRawSQL);
        }
      }
    }
    else {

     where_CLAUSE = !AND ? " WHERE TRUE " : ''
      _.forEach(whereObjOrRawSQL, function(value,key){
        if( _.isNull(value) || _.isUndefined(value) ){
          where_CLAUSE += " AND " + key + " IS NULL ";
        }
        else if( ( key.lastIndexOf('_id') === (key.length-3) || key.trim() === 'score' )   && parseInt(value) > 0  ){
          where_CLAUSE += " AND " + key + " = "+parseInt(value)+" ";
        }
        else if(  value instanceof Object && value instanceof Date ){
          where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
        }
        else if(  value instanceof Object && value.condition ){
          where_CLAUSE += " AND " + key + " "+ value.condition + " ";
        }
        else if( key === 'raw_postgresql' ) {
          where_CLAUSE += " " + value + " ";
        }
        else if(  typeof value === 'boolean' ){
         where_CLAUSE += " AND " + key + " IS "+ value + " ";
        }
        else {
          try {
            value = value.toString();
          } catch(e){
            console.error(e.stack);
            value = '';
          }
         where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
        }
      });
    }
  } catch(e){
    console.error(e.stack);
  }
  this.abstractTableWhere = where_CLAUSE;
}

GenerateWhereObj.prototype.getWhere = function(){
  return this.abstractTableWhere;
};


var AbstractTable = function(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnection){
  this.abstractTableDb = databaseName;
  var DB = this.abstractTableDb.toUpperCase();
  var CACHE_KEY = DB+CACHE;
  this.abstractTableDB = DB;
  this.databaseName = databaseName;
  this.databaseAddress = databaseAddress;
  this.databasePassword  = databasePassword;
  this.databasePort = databasePort;
  this.databaseUser = databaseUser;
  if(  typeof dbConnection !== 'object' ) {
    var err = new Error("Abstract Table client property in constructor not an object")
    throw err;
  }
  dbConnection.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser)
  this.PGClient = dbConnection
  createVirtualSchema(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,this.PGClient);
  this.abstractTableSchema = (process[CACHE_KEY][tablename]) || [];
  this.abstractTableTableName = tablename || undefined;
  this.abstractTablePrimaryKey = this.abstractTableSchema.indexOf(this.abstractTableTableName+"_id") > -1 ? this.abstractTableTableName+"_id" : undefined;
  this.initializeTable();
  createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(this.abstractTableSchema);

};

AbstractTable.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.PGClient.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser)
}

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
    AbstractTable.prototype['findBy'+camelizedColObj.functionId] = function(idIntegerParam){
      this.initializeTable();

      var camelizedColName = camelizedColObj.colName;

      if( parseInt(idIntegerParam).toString() === 'NaN' ) { this.error = new Error('findBy'+camelizedColObj.functionId + " first and only parameter must be a "+camelizedColName+" integer and it was => " + typeof idIntegerParam );
      }

      this.primaryKeyLkup = camelizedColName && camelizedColName === this.abstractTablePrimaryKey ? true : false;
      if( this.abstractTableQuery.trim().indexOf('select') === -1 )
        this.abstractTableQuery = "SELECT "+this.abstractTableTableName+".* FROM "+ this.abstractTableTableName + " " + this.abstractTableTableName;
      return this.where(camelizedColName+"="+idIntegerParam);
    };

    //getIds return 1 record if calling getAll<PrimaryKeyId>s without whereParams
    AbstractTable.prototype['getAll'+camelizedColObj.functionId+'s'] = function(whereParams){
      this.initializeTable();
      var camelizedColName = camelizedColObj.colName;
      this.primaryKeyLkup = _.isUndefined(whereParams) && camelizedColName === this.abstractTablePrimaryKey ? true : false;
      var DISTINCT = !_.isUndefined(whereParams) ? 'DISTINCT' : '';
      if(  this.primaryKeyLkup  ){
        DISTINCT = '';
      }
      this.abstractTableQuery = "SELECT "+DISTINCT+" "+this.abstractTableTableName+"."+camelizedColName+" FROM "+ this.abstractTableTableName + " " + this.abstractTableTableName;
      if( !_.isUndefined(whereParams) ){
        this.where(whereParams);
      }
      //console.log("this.abstractTableQuery",this.abstractTableQuery);
      return this;
    };
  });
}

AbstractTable.prototype.rawsql =  function (rawSql){
  this.initializeTable();
  this.abstractTableQuery = rawSql;
  return this;
};


AbstractTable.prototype.select =  function (selectParams){
  this.initializeTable();
  this.selecting = true;
  this.abstractTableWhere = '';
  var querySelect = '';
  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = selectParams.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if( rawSQLStr.indexOf('select') === 0 )
      querySelect = " "+rawSQLStr+" "; // select * from this.abstractTableTableName expected;
    else if ( rawSQLStr.indexOf('select') !== 0 )
      querySelect = "SELECT "+selectParams+" FROM "+this.abstractTableTableName;
    else
      querySelect = updateObjOrRawSQL;
  } else {
    if( ! ( selectParams instanceof Array ) ) selectParams = [];
    var tableName = this.abstractTableTableName;
    selectParams = _.map(selectParams,function(colName){
      if( colName.indexOf('.') > -1 ) return colName;
      else return tableName + "."+colName;
    });
    if( selectParams.length === 0 ) selectParams = ['*'];
    querySelect = "SELECT "+selectParams.join(' , ')+" FROM "+ this.abstractTableTableName+" "+this.abstractTableTableName + " ";
  }
  this.abstractTableQuery = querySelect;
  return this;
};

AbstractTable.prototype.selectAll = function(){
  this.initializeTable();
  return this.select("*");
};

AbstractTable.prototype.selectWhere = function(selectWhereParams,whereObjOrRawSQL){
  return this.select(selectWhereParams).where(whereObjOrRawSQL);
};

function externalJoinHelper(obj){
  var onCondition = "";
  _.forEach(obj,function(value,key){
    if( value instanceof Object && typeof value.condition === 'string' ){
      onCondition += " AND "+key+" "+value.condition+" ";
      return;
    }
    if( typeof value === 'boolean' ){
      onCondition += " AND "+key+" IS "+value+"  ";
      return;
    }
    if( typeof value === 'number' ){
      onCondition += "  AND "+key+" = "+value+" ";
      return;
    }
    onCondition += " AND "+key + " = " + value + " ";
  });
  return onCondition;
}

AbstractTable.prototype.join = function(tablesToJoinOnObjs){
  var self = this;
  var DB = self.abstractTableDB;
  var CACHE_KEY = DB+CACHE
  var rawSql = typeof tablesToJoinOnObjs === 'string' ? tablesToJoinOnObjs : null;
  var joinSQL = '';
  if( rawSql){
    joinSQL = " " + rawSql + " ";
  } else {
    var tables = tablesToJoinOnObjs;
    if( !( tables instanceof Object ) ){
      tables = {};
    }
    var thisTableName = this.abstractTableTableName;
    _.forEach(tables,function(obj,tablename){
      var schema = process[CACHE_KEY][tablename];
      obj.on = obj.on instanceof Array ? obj.on : [];
      var tableName = tablename;
      var alias = obj.as || tablename;
      var onArray = _(obj.on).chain().map(function(joinOnColumnsOrObj){
        if( typeof joinOnColumnsOrObj === 'string' && schema.indexOf(joinOnColumnsOrObj) > -1 ){
          return " AND "+alias+"."+joinOnColumnsOrObj+" = " +thisTableName+"."+joinOnColumnsOrObj+" ";
        }
        if( joinOnColumnsOrObj instanceof Object && _.keys(joinOnColumnsOrObj).length >= 1 ){
          return externalJoinHelper(joinOnColumnsOrObj);
        }
        return null;
      }).compact().value();
      //console.log("on Array",onArray)
      var onTrue = '';
      if( onArray.length === 0 ) onArray = ['false'];
      if( onArray.length > 0 ) onTrue = 'TRUE ';
      joinSQL = " "+( obj.type||'INNER' ).toUpperCase() +" "+"JOIN "+ tableName + " " + alias + " ON "+onTrue+" " + onArray.join(' ') + " ";
    });
  }

  this.abstractTableQuery += joinSQL;
  return this;
};




AbstractTable.prototype.insert = function(optionalParams){
  this.initializeTable();
  this.inserting = true;
  this.abstractTableQuery = "INSERT INTO " + this.abstractTableTableName + " ";
  if( optionalParams instanceof Object ){
    this.values(optionalParams);
  }
  return this;
};

AbstractTable.prototype.values = function(params){
  var self = this;
  var table_id = self.abstractTableTableName + "_id";
  var count = 1;
  var schema = self.abstractTableSchema;
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
      else if( value instanceof Object && typeof value.condition === 'string' ){
        ofTypeColumn = 'pgsql_condition';

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
            return "'" + utilityFunctions.escapeApostrophes(val) + "'";
          else
            return val;
        });
        ofTypeColumn = 'pgsql_function';
        value = functionToRun + "("+pgFunctionInputs.join(',')+") ";
      }
      else if( value instanceof Object && value instanceof Date ){
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
        value = utilityFunctions.escapeApostrophes(value);
      } else {
        try {
          value = value.toString();
          value = utilityFunctions.escapeApostrophes(value);
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
        case 'pgsql_condition':
          columnNames.push(key);
          fieldValue = value.condition;
          var comparisonOperator = "="
          if(typeof value.operator === 'string'){
            comparisonOperator = value.operator
          }
          selectValuesAs.push(" "+fieldValue + " as " + key+ " ");
          queryParams += " AND " + key + " " +comparisonOperator+ " " + fieldValue + " ";
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
  this.abstractTableQuery += " (" + columnNames.join(",") + ") SELECT " + selectValuesAs.join(', ') + " ";
  this.abstractTableWhereUniqueParams = queryParams;
  return this;
};

AbstractTable.prototype.unique = function(params){

  var whereUnique = " WHERE NOT EXISTS ( SELECT 1 FROM "+  this.abstractTableTableName + " WHERE true ";
  whereUnique += this.abstractTableWhereUniqueParams;
  whereUnique += " ) ";
  this.abstractTableQuery += whereUnique;

  return this;
};


AbstractTable.prototype.insertUnique = function(params){
  this.insert();
  this.values(params);
  this.unique();
  return this;
};

AbstractTable.prototype.update = function(updateObjOrRawSQL){

  this.initializeTable();
  this.updating = true;
  this.abstractTableQuery = '';
  var isRawSQL = typeof updateObjOrRawSQL === 'string' ? true : false;
  if( isRawSQL ){
    this.set(updateObjOrRawSQL);
  }
  this.abstractTableQuery  = 'UPDATE '+this.abstractTableTableName + ' ';

  if( updateObjOrRawSQL instanceof Object ) {
    this.set(updateObjOrRawSQL);
  }

  return this;
};


AbstractTable.prototype.set = function(updateObjOrRawSQL){

  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = updateObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if ( rawSQLStr.indexOf('set') !== 0 )
      this.abstractTableQuery += "SET "+updateObjOrRawSQL+" ";
    else
      this.abstractTableQuery += updateObjOrRawSQL.trim();
    return this;
  } else {
    var sql = "SET ";
    _.forEach(updateObjOrRawSQL,function(value,key){
      if( _.isNull(value) || _.isUndefined(value) )
        sql +=  key + " = NULL " + " , ";
      else if(  value instanceof Object && typeof value.condition === 'string' )
        sql += key + " "+ value.condition + " , ";
      else if(  value instanceof Object && value instanceof Date ){
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
        sql += key + " = " + " '"+utilityFunctions.escapeApostrophes(value)+"' " + " , ";
      }
    });
    sql = sql.slice(0, sql.lastIndexOf(" , "));
    this.abstractTableQuery += sql;
    return this;
  }
};


AbstractTable.prototype.deleteFrom = function(){
  this.initializeTable();
  this.deleting = true;
  //console.log("client");
  this.abstractTableQuery = "DELETE FROM "+this.abstractTableTableName+ " WHERE FALSE";
  return this;
};

AbstractTable.prototype.and = function(whereObjOrRawSQL){
  var self = this;
  var generatedWhereClause = new GenerateWhereObj(whereObjOrRawSQL,true);
  var whereQueryGenerated = generatedWhereClause.getWhere();
  //console.log("whereQueryGenerated",whereQueryGenerated)
  this.abstractTableWhere += ' '+whereQueryGenerated+' '
  this.abstractTableQuery += ' '+whereQueryGenerated+' ';
  return this;
}

AbstractTable.prototype.where = function(whereObjOrRawSQL){

  var selectTmp = this.abstractTableQuery.toLowerCase().trim().replace(/(\s{1,})/gm," ");
  if( !selectTmp && selectTmp.indexOf('select') === -1 && selectTmp.indexOf('update '+this.abstractTableTableName) === -1  && selectTmp.indexOf('delete from') === -1  ) {
    this.abstractTableQuery = "SELECT * FROM "+this.abstractTableTableName + " "+this.abstractTableTableName+ " ";
  }
  if( ! whereObjOrRawSQL ){
    this.primaryKeyLkup = false;
  }
  var generatedWhereClause = new GenerateWhereObj(whereObjOrRawSQL);
  var whereQueryGenerated = generatedWhereClause.getWhere();
  //console.log("whereQueryGenerated",whereQueryGenerated);
  if( typeof whereQueryGenerated === 'string' && whereQueryGenerated.length > 7 && this.deleting ){ // unlocking delete safety
    this.abstractTableQuery = this.abstractTableQuery.replace("DELETE FROM "+this.abstractTableTableName+" WHERE FALSE","DELETE FROM "+this.abstractTableTableName+" ");
  }
  this.abstractTableWhere = whereQueryGenerated;
  //console.log("where ->",whereQueryGenerated,'from ->',whereObjOrRawSQL);
  this.abstractTableQuery += (whereQueryGenerated||'');
  //this.optimizeQuery();
  return this;
};


AbstractTable.prototype.orderBy = function(arrOrRawOrderBy){

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

  this.abstractTableQuery += " ORDER BY ";
  orderByStr = orderByStr ? orderByStr : '1'; // default order by first param;
  this.abstractTableQuery += " " + orderByStr + " ";
  return this;
};

/*
 *                                                     // optional params          //optional param
 *  @usage .AndNotExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'})
 */
AbstractTable.prototype.AndNotExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL){
  return this.AndExists(tableNameExists,onColumnIds,whereExistsObjOrSQL,false);
}

/*
 *                                                     // optional params          //optional param
 *  @usage .AndExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'},true)
 */
AbstractTable.prototype.AndExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL,NOT){
  NOT = typeof NOT === 'boolean' && !NOT ? " NOT " : "";
  onColumnIds = _.isNull(onColumnIds) || _.isNull(onColumnIds) ? [] : onColumnIds;
  onColumnIds = onColumnIds instanceof Array ? onColumnIds : [onColumnIds];

  if( typeof whereExistsObjOrSQL === 'boolean' ) {
    NOT = whereExistsObjOrSQL ? '' : ' NOT ';
    whereExistsObjOrSQL = null;

  }

  var whereQuery = whereExistsObjOrSQL ? ( new GenerateWhereObj(whereExistsObjOrSQL) ).getWhere() : ' WHERE TRUE ';
  var mainTableName = this.abstractTableTableName;
  var whereOnColumnIdsAnd = onColumnIds.length > 0 ? " AND " : " ";
  whereQuery =  "AND "+NOT+" EXISTS (select 1 from "+tableNameExists+ " "+tableNameExists+" "+
                whereQuery+whereOnColumnIdsAnd+
                _.map(onColumnIds,function(colName){
                  return " "+tableNameExists+"."+colName+" = "+mainTableName+"."+colName + " ";
                }).join(" AND ") +
                " )";
  this.abstractTableWhere += " "+whereQuery+" ";
  this.abstractTableQuery += " "+whereQuery+" ";
  return this;
};

AbstractTable.prototype.groupBy = function(textOrObj){
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
  this.abstractTableQuery += " " + orderByStr + " ";
  return this;
};

AbstractTable.prototype.having = function(i){
  this.abstractTableQuery += " HAVING "+i+" ";
  return this;
};

AbstractTable.prototype.offset = function(i){

  this.abstractTableQuery += " OFFSET "+(parseInt(i)||0)+" ";
  return this;
};


AbstractTable.prototype.limit = function(i){

  this.abstractTableQuery += " LIMIT "+(parseInt(i)||'ALL')+" ";
  return this;
};


AbstractTable.prototype.optimizeQuery = function(){
  if( this.primaryKeyLkup ){
    this.limit(1);
  }
  return this;
};

AbstractTable.prototype.dbQuery = function(query,callback){
  var self = this;


  self.setConnectionParams(self.databaseName,self.databaseAddress,self.databasePassword,self.databasePort,self.databaseUser)
  self.PGClient.query(query,[],function(err,results){
      if(err) return callback(err,null);
      result = results instanceof Object && results.rows instanceof Array ? results.rows : [];
      callback(null,result);
  });

};

AbstractTable.prototype.dbQuerySync = function(query){
  var self = this;
  self.setConnectionParams(self.databaseName,self.databaseAddress,self.databasePassword,self.databasePort,self.databaseUser)
  var ret = self.PGClient.querySync.bind( self.PGClient )(query);
  ret.failed = ret.error ? true : false;
  ret.Rows = function () {  return ret.rows; };
  ret.Error = function () {  return ret.error; };
  return ret;
};

AbstractTable.prototype.run = function(callback){
  var self = this;


  self.finalizeQuery();

  var QueryToRun = self.abstractTableQuery + self.returnIds;

  var IS_PROMISED = typeof callback !== 'function';
  var q;
  if( IS_PROMISED ) q = Q.defer();


  callback = typeof callback === 'function' ? callback : function(){};

  this.initializeTable();


  if( self.error ){
    if(IS_PROMISED ) q.reject(self.error);
    else callback(self.error,null);
    self.initializeTable();
  } else {
    self.dbQuery(QueryToRun,function(err,rows){
      if(err ) {
        //console.error("Error query =>",Query);
        if(IS_PROMISED ) q.reject(err);
        else callback(err,null);
      }
      else {
        if(IS_PROMISED ) q.resolve(rows);
        else callback(null,rows);
      }
      self.initializeTable();
    });
  }
  if( IS_PROMISED) return q.promise;
};




AbstractTable.prototype.runSync = function(callback){

  var self = this;
  self.finalizeQuery.bind(self)();
  var QueryToRun = self.abstractTableQuery + self.returnIds;
  var ret = self.dbQuerySync.bind(self)(QueryToRun);
  callback = typeof callback === 'function' ? callback : function(){};
  this.initializeTable();// unbind



  var retObj = {};
  var rows = ret.Rows()
  var error = ret.Error();



  retObj.Rows = function(){ return rows; }
  retObj.Error = function(){ return error; }
  retObj.results = { error: error, rows: rows };

  callback(error,rows);



  return retObj;
};

AbstractTable.prototype.finalizeQuery = function(){
  var querySet = this.abstractTableQuery;
  var querySetTrimmed = querySet.trim();
  var queryFinalized = querySet.toLowerCase().trim().replace(/\s{1,}/gmi," ").trim();
  // console.log("queryFinalized",queryFinalized);

  if ( queryFinalized.indexOf("insert into "+this.abstractTableTableName) === 0  || queryFinalized.indexOf("update "+this.abstractTableTableName ) === 0 || queryFinalized.indexOf("delete from "+this.abstractTableTableName) ===0 ) {

    if( querySetTrimmed.lastIndexOf(";") == querySetTrimmed.length-1 && querySetTrimmed.length > 0  ){
      var query = this.abstractTableQuery.trim();
      this.abstractTableQuery = query.substring(0,query.length-1).trim()
    }
    // console.log("\nthis.abstractTableTableName",this.abstractTableTableName)
    if ( queryFinalized.indexOf("returning " + this.abstractTableTableName + "_id") === -1 && this.abstractTableSchema.indexOf(this.abstractTableTableName + "_id") > -1 ) {
      this.returnIds = " RETURNING " + this.abstractTableTableName + "_id ";
    }
    else if ( queryFinalized.indexOf("returning ") == -1 ) {
      this.returnIds = " RETURNING * ";
    }
  }
  return this;
};

AbstractTable.prototype.printQuery = function(ovrLog){
  var self = this;
  self.finalizeQuery.bind(self)();
  var QueryToPrint = self.abstractTableQuery + self.returnIds;
  var queryLog = "\nquery => " + QueryToPrint + "\n";
  if( ! ovrLog ){
    abstractDBLog(queryLog);
    return this;
  }
  console.log(queryLog);
  return this;
};

AbstractTable.prototype.initializeTable = function(callback){
  callback = typeof callback === 'function' ? callback : function(){};
  this.abstractTableQuery = '';
  this.primaryKeyLkup = false;
  this.abstractTableWhere = '';
  this.abstractTableWhereUniqueParams = ''
  this.deleting = false;
  this.inserting = false;
  this.updating = false;
  this.returnIds = '';
  this.upserting = false;
  this.utilReady = false;
  this.error = null;
  callback();
  return this;
};


AbstractTable.prototype.util = function(){
  var self = this;
  self.initializeTable.bind(self)();
  this.initializeTable();

  self.utilReady = true;

  return self;
};

AbstractTable.prototype.upsert = function(setParams,whereParams,callback){
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  var q = Q.defer();
  var err = null;
  if( !self.utilReady ){
    err = new Error("Need to call util() before accessing utility functions")
  }
  else if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) {
    err = new Error("Can only insert or update object params")
  }

  var tableNameId = "*";
  var ret = [];
  try {
    tableNameId = self.abstractTableSchema.indexOf(self.abstractTableTableName+"_id") > -1 ? self.abstractTableTableName+"_id" : "*";
  } catch(e){
    err = e;
  }


  async.series([
    function checkReadyForUpsert(scb){
      if(err) return scb(err);
      scb();
    },
    function update(scb){
      self.update.bind(self)()
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
      self.insert.bind(self)()
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
      self.select.bind(self)([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    self.initializeTable.bind(self)();
    if(err) { q.reject(err); return callback(err); }
    q.resolve(ret||[]);
    callback(err||null,ret||[]);
  });


  return q.promise;
};


AbstractTable.prototype.upsertUsingColumnValues = function(setParams,whereParams,callback){
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  // console.log("self",_.keys(self))
  var err = null;
  var q = Q.defer();
  if( !self.utilReady ){
    err = new Error("Need to call util() before accessing utility functions");
  }
  else if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) {
    err = new Error("Can only insert or update object params")
  }

  var tableNameId = "*";
  var ret = [];

  try {
    tableNameId = self.abstractTableSchema.indexOf(self.abstractTableTableName+"_id") > -1 ? self.abstractTableTableName+"_id" : "*";
  } catch(e){
    err = e;
  }

  async.series([
    function checkReadyForUpsert(scb){
      if(err) return scb(err);
      scb();
    },
    function update(scb){
      self.update.bind(self)()
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
      self.insert.bind(self)()
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
      self.select.bind(self)([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    self.initializeTable.bind(self)();
    if(err) { q.reject(err); return callback(err); }
    q.resolve(ret||[]);
    callback(err||null,ret||[]);
  });

  return q.promise;
};





module.exports = AbstractTable;
module.exports.createVirtualSchema = createVirtualSchema
