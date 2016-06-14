var PG_JC_HOME = __dirname
var _ = require('lodash');
var async = require('async');
var Q = require('q');
var fs = require('fs');
var config = require("./config");
var inflection = require('inflection');

var NODE_ENV = config.ENV;
var DB_LOG = config.DB_LOG_ON;
var utilityFunctions = {
  console: { asyncLog: function(){
      var args = _.values(arguments);
      setTimeout(function(){
        console.log.apply(this,args)
      },10)
    }
  },
  escapeApostrophes: function(str){
    if( typeof str != 'string' ){
      throw "not a string"
    }
    return str.replace(/\'/gm,"''")
  }
}
var abstractDBLog = NODE_ENV !== 'production' && DB_LOG ? utilityFunctions.console.asyncLog : function(){};


var DUPLICATE_KEY_VIOLATION = '23505';
var FS_DATABASE_SCHEMA_SUFFIX = "_TABLES_SCHEMA_CACHE";

function schemaQuery(dbName,tableSchema){
  tableSchema = tableSchema ? tableSchema : 'public'
  return "select * from (select tt.table_name as tablename, array_agg( \
(select row_to_json(_) from ( select col.column_name,col.data_type, \
case when col.data_type in ('text','varchar','character varying') then 'string' \
when col.data_type in ('bigint','integer','numberic','real','double precision') then 'number' \
when col.data_type in ('timestamp without time zone','timestamp with time zone') then 'time' \
when col.data_type in ('date') then 'date' \
when col.data_type in ('boolean') then 'boolean' \
when col.data_type in ('json','ARRAY') then 'object' \
end as js_type, \
case when col.column_name = ccu.column_name then true else false end as is_primary_key \
) as _ ) ) as tableschema \
from information_schema.tables tt \
join information_schema.columns col on col.table_name = tt.table_name \
left join information_schema.table_constraints tc on tc.table_name = tt.table_name and tc.constraint_type = 'PRIMARY KEY' \
left JOIN information_schema.constraint_column_usage AS ccu ON tc.constraint_name = ccu.constraint_name \
where tt.table_catalog = '"+dbName+"' \
and tt.table_schema = '"+tableSchema+"' \
group by tt.table_name \
) tables \
order by tables.tablename;";
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

function getConnectionFSCacheKey(dbName,dbAddr,dbPort,dbUser){
  var filename = dbName+"_"+dbAddr.replace(/\./g,"-")+"_"+dbPort+"_"+dbUser + FS_DATABASE_SCHEMA_SUFFIX;
  filename = filename.toUpperCase()
  return filename;
}


function createVirtualSchema(dbName,dbAddr,dbPasswd,dbPort,dbUser,dbConnection){
  // var start = process.hrtime();
  var DB = dbName.toUpperCase();
  var CACHE_KEY = getConnectionFSCacheKey(dbName,dbAddr,dbPort,dbUser);
  var schemaTmp = {};
  // console.log("process[CACHE_KEY]",process[CACHE_KEY])
  var setCache = isCacheNotSet(CACHE_KEY)

  if( setCache ) {
    // console.log("dbName,dbAddr",dbName,dbAddr)
    // console.log("dbConnection",dbConnection)
    // console.log("setCache",CACHE_KEY,setCache)
    process[CACHE_KEY] = process[CACHE_KEY] || {};
    var data = dbConnection.querySync("select 1 first_db_call_test, '"+dbConnection.databaseAddress+"' as address,'"+dbConnection.databaseName+"' as database")
    initErrorHandler(data.error)
    try { schemaTmp = dbConnection.querySync(schemaQuery(dbName)); } catch(e){ console.error(e.stack); }
    // dbConnection.query("select 1");
    // console.log("schemaTmp",schemaTmp)
    var pathToFileCache = PG_JC_HOME + '/schemas/'+CACHE_KEY+'.json';
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
    // console.log("process[CACHE_KEY]",process[CACHE_KEY])
    _.each(schemaTmp,function(obj){
      // console.log('process[CACHE_KEY][obj.tablename]',obj.tablename,process[CACHE_KEY][obj.tablename])
      process[CACHE_KEY][obj.tablename] = _.cloneDeep( obj.tableschema );
    });
    // dbConnection.logoutSyncClient();
  } else {
    // abstractDBLog("Using memory DB cache");
  }
  // var t= process.hrtime(start);
  // abstractDBLog("Query took: "+t[0]+":"+t[1].toString().slice(0,2)+"s  => loading schema");
}

var GenerateWhereObj = function (tableName,tableSchema,whereObjOrRawSQL,AND){

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
    else if ( whereObjOrRawSQL instanceof Object ){
      var schemaData = getTableSchemaDataMap(whereObjOrRawSQL,tableSchema)
      var keys = schemaData.columnNames;
      var paramsData = schemaData.paramsData;
      var columnNamesData = schemaData.columnDataMap



      where_CLAUSE = !AND ? " WHERE TRUE " : ''
      _.forEach(whereObjOrRawSQL, function(value,key){

        var paramData = columnNamesData[key] || {};

        switch(true){
          case ( key === 'raw_postgresql' || key === 'raw_sql' ):
            if( typeof value === 'string' ){
                where_CLAUSE += " " + value + " ";
            } else {
              console.error(new Error(tableName+ " "+key+ " not sql where clause ").stack)
            }
            break;
          case ( value instanceof Object && typeof value.condition === 'string' ):
            where_CLAUSE += " AND " + key + " "+ value.condition + " ";
            break;
          case _.isNull(value) || _.isUndefined(value):
            where_CLAUSE += " AND " + key + " IS NULL ";
            break;
          case ( paramData.js_type == 'number' ):
            if( !isNaN( parseInt(value) ) ){
              where_CLAUSE += " AND " + key + " = "+value+" ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'date'  ):
            if( (value instanceof Date) && value !== 'Invalid Date' ){
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::DATE ";
            } else if ( typeof value == 'string' && new Date(value) !== 'Invalid Date' ) {
              value = new Date(value)
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::DATE ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
          case ( paramData.js_type == 'time'  ):
            if( (value instanceof Date) && value !== 'Invalid Date' ){
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
            } else if ( typeof value == 'string' && new Date(value) !== 'Invalid Date' ) {
              value = new Date(value)
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'object'  ):
            if( (value instanceof Object || value instanceof Array) ){
              value = JSON.stringify(value)
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            } else {
              console.error(new Error(tableName+ " "+key+ " not an "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'string'  ):
            if( typeof value === 'string' ){
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            }
            else if ( value != null && typeof value !== 'undefined' && typeof value.toString === 'function' && value.toString() ){
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value.toString()) + "' ";
            }
            else {
              console.error(new Error(tableName+ " "+key+ " not a "+paramData.js_type+" "+value).stack)
            }
            break;
          default:
            try {
              value = value.toString();
            } catch(e){
              console.error(e.stack);
              value = '';
            }
            where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            break;
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
  var CACHE_KEY = getConnectionFSCacheKey(databaseName,databaseAddress,databasePort,databaseUser);
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
  this.abstractTableTableSchema = (process[CACHE_KEY][tablename]) || [];
  this.abstractTableTableName = tablename || undefined;
  this.abstractTablePrimaryKey = (_(this.abstractTableTableSchema).chain().filter(function(s){ return s.is_primary_key }).compact().head().value() || {}).column_name || null
  this.initializeTable();
  createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(this.abstractTableTableSchema);

};

AbstractTable.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.PGClient.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser)
}

function createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(schema){
  var idColumns = _.compact(_.map(schema,function(obj){
    var endsIn_id =  obj.column_name.lastIndexOf('_id') === (obj.column_name.length-3) ;
    if( endsIn_id  )
      return { functionId: inflection.camelize(obj.column_name), colName: obj.column_name };
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

      if( ! isNaN( parseInt(idIntegerParam) ) ) { this.error = new Error('findBy'+camelizedColObj.functionId + " first and only parameter must be a "+camelizedColName+" integer and it was => " + typeof idIntegerParam );
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

function externalJoinHelper(obj,schema){
  // console.log("JOIN ON",obj)
  var schemaData = getTableSchemaDataMap(obj,schema)
  var keys = schemaData.columnNames;
  var paramsData = schemaData.paramsData;
  var columnNamesData = schemaData.columnDataMap


  var onCondition = "";

  _.forEach(obj,function(value,key){
    var paramData = columnNamesData[key] || {}
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
  var CACHE_KEY = getConnectionFSCacheKey(self.databaseName,self.databaseAddress,self.databasePort,self.databaseUser);
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
      var schema = process[CACHE_KEY][tablename] || [];

      if( schema.length == 0 ){
          console.log("schema",schema,tablename)
      }


      obj.on = obj.on instanceof Array ? obj.on : [];
      var tableName = tablename;
      var alias = obj.as || tablename;
      var onArray = _(obj.on).chain().map(function(joinOnColumnsOrObj){
        if( typeof joinOnColumnsOrObj === 'string' && _(schema).chain().filter(function(o){ return joinOnColumnsOrObj.indexOf(o.column_name) === joinOnColumnsOrObj.replace(o.column_name,"").length  }).compact().head().value() instanceof Object ){
          return " AND "+alias+"."+joinOnColumnsOrObj+" = " +thisTableName+"."+joinOnColumnsOrObj+" ";
        }
        if( joinOnColumnsOrObj instanceof Object && _.keys(joinOnColumnsOrObj).length >= 1 ){
          return externalJoinHelper(joinOnColumnsOrObj,schema);
        }
        return null;
      }).compact().value();
      // console.log("on Array",onArray)
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



function getTableSchemaDataMap(params,schema){
  var self = this;
  var tableschema = schema || self.abstractTableTableSchema || []
  var retObj = {}
  var paramObj = {}
  var columnNames = [];
  _(_.keys(params)).chain().map(function(col){
    var colObj =  _(tableschema).chain().filter(function(o){ return col.indexOf(o.column_name) === col.replace(o.column_name,"").length }).compact().head().value()
    var isObj = colObj instanceof Object
    if( isObj ) {
      retObj[col] = colObj
      columnNames.push(colObj.column_name);
      paramObj[col] = params[col];
    }
  }).compact().value()

  var ret =  { paramsData: paramObj, columnNames: columnNames,  columnDataMap: retObj,  };

  // console.log("ret",ret)
  return ret;
}

AbstractTable.prototype.getTableSchemaDataMap = getTableSchemaDataMap
AbstractTable.prototype.values = function(params){
  var self = this;

  var count = 1;

  var schemaData = self.getTableSchemaDataMap.bind(self)(params)
  var keys = schemaData.columnNames;
  var paramsData = schemaData.paramsData;
  var columnNamesData = schemaData.columnDataMap


  if( keys.length === 0 )  {  this.error = new Error("No insert values passed"); return this; }
  var queryParams = "";
  var columnNames = [];
  var selectValuesAs = [];

  _.forEach(params, function(value,key){
    try {
      if( !key ) return;
      var paramData = columnNamesData[key] || {}
      var ofTypeColumn = '';
      var fieldValue = '';

      switch(true){
        case ( _.isNull(value) || _.isUndefined(value) ):
          ofTypeColumn = 'null';
          break;
        case ( value instanceof Object && typeof value.condition === 'string' ):
          ofTypeColumn = 'pgsql_condition';
          break;
        case ( value instanceof Object && value.pgsql_function instanceof Object ):
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
          break;
        case  ( paramData.js_type === 'object' && (value instanceof Object || value instanceof Array ) ):
          ofTypeColumn = 'object';
          value = utilityFunctions.escapeApostrophes( JSON.stringify(value) );
          break;
        case ( paramData.js_type === 'time' && ( value instanceof Date ) ):
          ofTypeColumn = 'date';
          break;
        case ( paramData.js_type === 'date' && ( new Date(value) instanceof Date) ):
          value = new Date(value);
          ofTypeColumn = 'date';
          break;
        case ( paramData.js_type === 'number' && !isNaN(parseInt(value)) ):
          ofTypeColumn = 'num' ;
          break;
        case ( paramData.js_type === 'boolean'  ):
          ofTypeColumn = 'bool';
          break;
        case ( paramData.js_type === 'string' ):
          ofTypeColumn = 'text';
          if( value !== null && typeof value !== 'undefined' && typeof value != 'string' && value.toString() ){
            value = value.toString()
          }
          value = utilityFunctions.escapeApostrophes(value);
          break;
        default:
          try {
            value = value.toString();
            value = utilityFunctions.escapeApostrophes(value);
            ofTypeColumn = 'text';
          } catch(e){
            console.error(e.stack)
            ofTypeColumn = 'null';
          }
          break;
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
        case 'num':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" "+fieldValue+" as " + key+" ");
          queryParams += " AND " + key + " = " + fieldValue + " ";
          break;
        case 'null':
          columnNames.push(key);
          fieldValue = null;
          selectValuesAs.push(" null as " + key+" ");
          queryParams += " AND " + key + " IS NULL ";
          break;
        case 'object':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" '"+fieldValue + "' as " + key+ " ");
          queryParams += " AND " + key + "::TEXT = '" + fieldValue + "'::TEXT ";
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
          console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid entry ").stack)
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
  var self = this;
  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = updateObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if ( rawSQLStr.indexOf('set') !== 0 )
      this.abstractTableQuery += "SET "+updateObjOrRawSQL+" ";
    else
      this.abstractTableQuery += updateObjOrRawSQL.trim();
    return this;
  } else if ( updateObjOrRawSQL instanceof Object ){
    var sql = "SET ";
    var schemaData = this.getTableSchemaDataMap.bind(this)(updateObjOrRawSQL)
    var keys = schemaData.columnNames;
    var paramsData = schemaData.paramsData;
    var columnNamesData = schemaData.columnDataMap

    _.forEach(updateObjOrRawSQL,function(value,key){

      var paramData = columnNamesData[key] || {}

      switch (true) {
        case ( _.isNull(value) || _.isUndefined(value) ):
          sql +=  key + " = NULL " + " , ";
          break;
        case (  value instanceof Object && typeof value.condition === 'string' ):
          sql += key + " "+ value.condition + " , ";
          break;
        case (  paramData.js_type === 'date'   ):
          if(value instanceof Date && value != "Invalid Date"){
            sql += key + " = '"+ value.toISOString() + "'::DATE , ";
          }
          else if (typeof value === 'string' && new Date(value) != "Invalid Date" ){
            value = new Date(value)
            sql += key + " = '"+ value.toISOString() + "'::DATE , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case (  paramData.js_type === 'time'   ):
          if(value instanceof Date && value != "Invalid Date"){
            sql += key + " = '"+ value.toISOString() + "'::TIMESTAMP , ";
          }
          else if (typeof value === 'string' && new Date(value) != "Invalid Date" ){
            value = new Date(value)
            sql += key + " = '"+ value.toISOString() + "'::TIMESTAMP , ";
          }
          else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case( paramData.js_type === 'boolean' ):
          if( typeof value === 'boolean' ) {
            sql += key + " = " + value + " , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;

        case ( paramData.js_type === 'number' ):
          if( ! isNaN( parseInt(value) ) ){
              sql += key + " = "+ value + " , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case ( paramData.js_type === 'object' ):
          if( (value instanceof Object || value instanceof Array) ){
              value = JSON.stringify(value)
              value = utilityFunctions.escapeApostrophes(value)
              sql += key + " = " + " '"+value+"' " + " , ";
          }
          else if( typeof value === 'string' ){
            value = utilityFunctions.escapeApostrophes(value)
            sql += key + " = " + " '"+value+"' " + " , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case ( paramData.js_type === 'string' ):
          if( typeof value === 'string' ){
              value = utilityFunctions.escapeApostrophes(value)
              sql += key + " = " + " '"+value+"' " + " , ";
          }
          else if ( value != null && typeof value !== 'undefined' && typeof value.toString == 'function' && value.toString() ) {
            value = utilityFunctions.escapeApostrophes(value.toString())
            sql += key + " = " + " '"+value+"' " + " , ";
          }
          else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value + " toString() -> "+ value.toString() ).stack)
          }
          break;
        default:
          try {
            value = value.toString();
          } catch(e) {
            console.error(e.stack);
            value = '';
          }
          sql += key + " = " + " '"+utilityFunctions.escapeApostrophes(value)+"' " + " , ";
          break;
      }
    });
    sql = sql.slice(0, sql.lastIndexOf(" , "));
    this.abstractTableQuery += sql;

  }

  return this;
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
  var generatedWhereClause = new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereObjOrRawSQL,true);
  var whereQueryGenerated = generatedWhereClause.getWhere();
  //console.log("whereQueryGenerated",whereQueryGenerated)
  this.abstractTableWhere += ' '+whereQueryGenerated+' '
  this.abstractTableQuery += ' '+whereQueryGenerated+' ';
  return this;
}

AbstractTable.prototype.where = function(whereObjOrRawSQL){
  var self = this;
  var selectTmp = this.abstractTableQuery.toLowerCase().trim().replace(/(\s{1,})/gm," ");
  if( !selectTmp && selectTmp.indexOf('select') === -1 && selectTmp.indexOf('update '+this.abstractTableTableName) === -1  && selectTmp.indexOf('delete from') === -1  ) {
    this.abstractTableQuery = "SELECT * FROM "+this.abstractTableTableName + " "+this.abstractTableTableName+ " ";
  }
  if( ! whereObjOrRawSQL ){
    this.primaryKeyLkup = false;
  }
  var generatedWhereClause = new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereObjOrRawSQL);
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
  var self = this;
  NOT = typeof NOT === 'boolean' && !NOT ? " NOT " : "";
  onColumnIds = _.isNull(onColumnIds) || _.isNull(onColumnIds) ? [] : onColumnIds;
  onColumnIds = onColumnIds instanceof Array ? onColumnIds : [onColumnIds];

  if( typeof whereExistsObjOrSQL === 'boolean' ) {
    NOT = whereExistsObjOrSQL ? '' : ' NOT ';
    whereExistsObjOrSQL = null;

  }

  var whereQuery = whereExistsObjOrSQL ? ( new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereExistsObjOrSQL) ).getWhere() : ' WHERE TRUE ';
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
    if ( ! _.isNull ( this.abstractTablePrimaryKey ) ) {
      this.returnIds = " RETURNING " + this.abstractTablePrimaryKey;
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
    tableNameId = self.abstractTablePrimaryKey || "*"
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
    tableNameId = self.abstractTablePrimaryKey || "*"
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
module.exports.getConnectionFSCacheKey = getConnectionFSCacheKey;
