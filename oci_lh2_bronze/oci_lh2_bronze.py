import socket
import os
import os.path

import pandas as pd

from nlstools.config_settings import *
from nlstools.tool_kits import *
from nlsdb.dbwrapper_factory import *
from nlsfilestorage.filestorage_wrapper_factory import *

EXPLOIT_ARG_LOADING_TABLE = 'l'
EXPLOIT_ARG_LOG_TABLE = 'o'
EXPLOIT_ARG_RELOAD_ON_ERROR_INTERVAL = 'x'
EXPLOIT_ARG_NOT_DROP_TEMP_RUNNING_LOADING_TABLE = 'k'


PARQUET_IDX_DIGITS = 5
PANDAS_CHUNKSIZE = 100000
# Variables could be redefined by json config file
DB_ARRAYSIZE = 50000
SQL_READMODE = "DBCURSOR" #CURSORDB / PANDAS
PARQUET_FILE_EXTENSION = ".parquet"

DBFACTORY = NLSDbFactory()
FILESTORAGEFACTORY = NLSFileStorageFactory()

SOURCE_PPROPERTIES_SYNONYMS = {'SRC_TYPE': 'type', 'SRC_NAME': 'name', 'SRC_ORIGIN_NAME': 'schema', 'SRC_OBJECT_NAME': 'table', 'SRC_OBJECT_CONSTRAINT': 'table_constraint', 'SRC_FLAG_ACTIVE': 'active', 'SRC_FLAG_INCR': 'incremental', 'SRC_DATE_CONSTRAINT': 'date_criteria', 'SRC_DATE_LASTUPDATE': 'last_update', 'FORCE_ENCODE': 'force_encode', 'BRONZE_POST_PROCEDURE': 'bronze_post_proc', 'BRONZE_POST_PROCEDURE_ARGS': 'bronze_post_proc_args'}
SourceProperties = namedtuple('SourceProperties',list(SOURCE_PPROPERTIES_SYNONYMS.values()))
# SourceProperties namedtuple is set into Exploit __init__, based on fields of table used to list sources

BronzeProperties = namedtuple('BronzeProperties',['environment','schema','table','bucket','bucket_filepath','parquet_template'])

class BronzeConfig():
    #Define configuration envrionment parameters to execute.
    #Mainly extract parameters from json configuration_file
    #Provide a generic method to get an oracledb connection, depending on database options defined into json

    def __init__(self,configuration_file):
        self.configuration_file = path_replace_tilde_with_home(configuration_file)
        self.options = get_parser_config_settings("options")(self.configuration_file,"options")
        
        self.duckdb_settings = get_parser_config_settings("duckdb_settings")(self.configuration_file,"duckdb_settings")
        
        if self.options.db_arraysize.isdigit():
            global DB_ARRAYSIZE
            DB_ARRAYSIZE = eval(self.options.db_arraysize)

        if not self.options.sql_readmode:
            global SQL_READMODE
            SQL_READMODE = self.options.sql_readmode

        if self.options.environment == "DEBUG":
            self.debug = True
            self.oci_settings = get_parser_config_settings("filestorage")(self.configuration_file,"BRONZE_BUCKET_DEBUG")
        else:
            self.debug = False
            self.oci_settings = get_parser_config_settings("filestorage")(self.configuration_file,"BRONZE_BUCKET")

        if self.options.rootdir != '':
            self.rootdir = path_replace_tilde_with_home(self.options.rootdir)
            os.chdir(self.rootdir)
        else:
            self.rootdir = ''

        # Create a temporary directory if it doesn't exist
        self.tempdir = self.options.tempdir
        if not os.path.exists(self.tempdir):
            os.makedirs(self.tempdir)

        # return absolute Path for log file.
        # if full path is not specified, then add workging dir ahead
        if not os.path.dirname(self.get_options().verboselogfile):
            self.verboselogfile = os.path.join(self.get_tempdir(),self.get_options().verboselogfile)
        else:
            self.verboselogfile = self.get_options().verboselogfile
        
        self.verboselevel = self.get_options().verbose_level

    def get_configuration_file(self):
        return self.configuration_file

    def get_options(self):
        return self.options

    def get_duckdb_settings(self):
        return self.duckdb_settings

    def get_oci_settings(self):
        return self.oci_settings
            
    def isdebugmode(self):
        return self.debug

    def get_rootdir(self):
        return self.rootdir
    def get_tempdir(self):
        return self.tempdir
    def get_verboselogfile(self):
        return self.verboselogfile
    def get_verboselevel(self):
        return self.verboselevel
class BronzeExploit:
    # Iterator object for list of sources to be imported into Bronze
    # Define metohd to update src_dat_lastupdate for table with incremental integration

    def __init__(self,pBronze_config,pVerbose=None,**optional_args):
        self.idx = 0
        self.verbose = pVerbose
        self.exploit_config = pBronze_config
        self.exploit_db_param = get_parser_config_settings("database")(self.exploit_config.get_configuration_file(),"exploit")
        self.exploit_db:absdb = DBFACTORY.create_instance(self.exploit_db_param.dbwrapper,self.exploit_config.get_configuration_file())
        self.exploit_db_connection = self.exploit_db.create_db_connection(self.exploit_db_param)

        self.exploit_loading_table = self.exploit_config.get_options().datasource_load_tablename_prefix + self.exploit_config.get_options().environment
        vCursor = self.get_db_connection().cursor()

        # drop temporary running loding table or Not
        self.not_drop_running_loading_table = optional_args[EXPLOIT_ARG_NOT_DROP_TEMP_RUNNING_LOADING_TABLE]

        if not optional_args[EXPLOIT_ARG_LOADING_TABLE] :
            # Create running/temporary List Datasource loading table.
            # Insert list of tables to import
            hostname = socket.gethostname()
            pid = os.getpid()
            # Temporary table name
            self.exploit_running_loading_table = 'TEMP_' + self.exploit_loading_table + "_" + str(hostname) + "_" + str(pid)
            self.exploit_running_loading_table = self.exploit_running_loading_table.replace('-','_')
            #proc_out_duplicate_table = vCursor.var(bool)
            #proc_out_createlh2_datasource = vCursor.var(str)

            exploit_running_loading_table_fullname = self.exploit_db_param.p_username +'.' + self.exploit_running_loading_table

            message = "Create running Exploit table  {}".format(self.exploit_running_loading_table)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=message)

            if optional_args[EXPLOIT_ARG_RELOAD_ON_ERROR_INTERVAL] and optional_args[EXPLOIT_ARG_LOG_TABLE] :
                vDmbs_output = self.get_db().execute_proc('ADMIN.DUPLICATE_TABLE', *[self.get_db_parameters().p_username, self.exploit_loading_table,self.get_db_parameters().p_username,self.exploit_running_loading_table,False])

                vInterval_start = optional_args[EXPLOIT_ARG_RELOAD_ON_ERROR_INTERVAL][0].strftime("%Y-%m-%d %H:%M:%S")
                # If end date of intervant not provided, then take Now date
                try:
                    vInterval_end = optional_args[EXPLOIT_ARG_RELOAD_ON_ERROR_INTERVAL][1].strftime("%Y-%m-%d %H:%M:%S")
                except IndexError:
                    vInterval_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                vLog_table = optional_args[EXPLOIT_ARG_LOG_TABLE]
                message = "Populate Exploit table {} with previous error tables from log table {} on interval {}->{}".format(self.exploit_running_loading_table,vLog_table,vInterval_start,vInterval_end)
                if self.verbose:
                    self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=message)
                vDmbs_output = self.get_db().execute_proc('CREATE_LH2_DATASOURCE_LOADING_ON_ERROR',*[self.exploit_loading_table,self.exploit_running_loading_table,vLog_table,vInterval_start,vInterval_end])
                self.get_db_connection().commit()
            else:
                vDmbs_output = self.get_db().execute_proc('ADMIN.DUPLICATE_TABLE', *[self.get_db_parameters().p_username, self.exploit_loading_table,self.get_db_parameters().p_username,self.exploit_running_loading_table,True])
                self.get_db_connection().commit()
            if not self.get_db().last_execute_proc_completion():
                message = "ERROR, Create running Exploit table  {}".format(self.exploit_running_loading_table)
                message += "\n{}".format(self.get_db().last_execute_proc_output())
                if self.verbose:
                    self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "ERROR", log_message=message)
                raise Exception(message)
        else:
            self.exploit_running_loading_table = optional_args[EXPLOIT_ARG_LOADING_TABLE]
            message = "Using running Exploit table  {}".format(self.exploit_running_loading_table)
            message += "\n{}".format(self.get_db().last_execute_proc_output())
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=message)

        #define SourceProperties namedtuple as a global type
        global SourceProperties
        vLoadingTableProperties = self.get_db().create_namedtuple_from_table('SourceProperties',self.exploit_running_loading_table)
        vNew_fields = [SOURCE_PPROPERTIES_SYNONYMS.get(old_name, old_name) for old_name in vLoadingTableProperties._fields]
        vNew_fields.append('request')
        vDefaults_values = [None] * len(vNew_fields)
        SourceProperties = namedtuple ('SourceProperties',vNew_fields,defaults=vDefaults_values)
        
        # Execute a SQL query to fetch activ data from the table "LIST_DATASOURCE_LOADING_..." into a dataframe
        param_req = "select * from " + self.exploit_running_loading_table + " where SRC_FLAG_ACTIV = 1 ORDER BY SRC_TYPE,SRC_NAME,SRC_OBJECT_NAME"
        vCursor.execute(param_req)
        self.df_param = pd.DataFrame(vCursor.fetchall())
        self.df_param.columns = [x[0] for x in vCursor.description]
        #self.iterator = iter(map(SourceProperties._make,vCursor.fetchall()))
        vCursor.close()

    def __del__(self):
        if not self.not_drop_running_loading_table:
            self.get_db().execute_proc('ADMIN.DROP_TABLE', *[self.get_db_parameters().p_username,self.exploit_running_loading_table])
            message = "Deleting temporary Exploit table  {}".format(self.exploit_running_loading_table)
            message += "\n{}".format(self.get_db().last_execute_proc_output())
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "END", log_message=message)
   
    def __iter__(self):
        return self

    def __next__(self):
        try:
            #items = [self.df_param.iloc[self.idx,i] for i in range(len(self.df_param.columns))]
            vLine = self.df_param.iloc[self.idx].tolist()
            items = SourceProperties(*vLine)
        except IndexError:
            raise StopIteration()
        self.idx += 1
        return items
        
        #return next(self.iterator)

    def get_db(self) -> absdb:
        return self.exploit_db
    
    def get_db_parameters(self):
        return self.exploit_db_param
    
    def get_db_connection(self):
        return self.exploit_db_connection
    
    def get_loading_tables(self):
        return (self.exploit_loading_table,self.exploit_running_loading_table)

    def update_exploit(self,pSource:SourceProperties,PColumn_name, pValue):
        request = ""
        try:
            vCursor = self.get_db_connection().cursor()
            request = "UPDATE " + self.exploit_loading_table + " SET "+PColumn_name+" = :1 WHERE SRC_NAME = :2 AND SRC_ORIGIN_NAME = :3 AND SRC_OBJECT_NAME = :4"

            # update last date or creation date (depends on table)
            message = "Updating {} = {} on table {}".format(PColumn_name,pValue,self.exploit_loading_table)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "SET_LASTUPDATE", "START", log_message=message,
                            log_request=request)
            bindvars = (pValue,pSource.name,pSource.schema,pSource.table)
            vCursor.execute(request,bindvars)
            self.get_db_connection().commit()
            vCursor.close()
            return True
        except oracledb.Error as err:
            vError = "ERROR {} with values {}".format(request,bindvars)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", vError,log_message='Oracle DB error : {}'.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            return False
        except Exception as err:
            vError = "ERROR {} with values {}".format(request, bindvars)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", vError,str(err))
            self.logger.log(pError=err, pAction=vError)
            return False

    def __str__(self):
        return f"BronzeExploit: exploit_running_loading_table={self.exploit_running_loading_table}, df_param={self.df_param}"

class BronzeLogger():
    def __init__(self, pBronze_config,pVerbose=None):
        self.logger_linked_bronze_source = None
        self.logger_linked_bronze_config = pBronze_config
        self.verbose = pVerbose
        self.logger_oracledb_connection = None
        if self.logger_linked_bronze_config:
            self.logger_env = pBronze_config.get_options().environment
            self.logger_table_name = self.logger_linked_bronze_config.get_options().log_table_prefix + self.logger_linked_bronze_config.get_options().environment
        self._init_logger()

    def _init_logger(self):
        if self.logger_linked_bronze_config:
            # Establish a connection to EXPLOIT schema to log message
            self.logger_db_param = get_parser_config_settings("database")(self.logger_linked_bronze_config.get_configuration_file(),
                                                                           self.logger_linked_bronze_config.get_options().logger_db)
            self.logger_db:absdb = DBFACTORY.create_instance(self.get_db_parameters().dbwrapper,self.logger_linked_bronze_config.get_configuration_file())

            self.logger_oracledb_connection = self.get_db().create_db_connection(self.get_db_parameters())

            # Set Process_info = hostname:user:pid
            vProcess_info = "{}:{}:{}".format(socket.gethostname(),os.getlogin(),os.getpid())
            self.BronzeLoggerProperties = self.get_db().create_namedtuple_from_table('BronzeLoggerProperties',self.logger_table_name)
            #self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),END_TIME=None, ENVIRONMENT=self.env,ACTION='',SRC_NAME='',SRC_ORIGIN_NAME='',SRC_OBJECT_NAME='',REQUEST='',ERROR_TYPE='',ERROR_MESSAGE='',STAT_ROWS_COUNT=0,STAT_ROWS_SIZE=0,STAT_TOTAL_DURATION=0,STAT_FETCH_DURATION=0,STAT_UPLOAD_PARQUETS_DURATION=0,STAT_SENT_PARQUETS_COUNT=0,STAT_SENT_PARQUETS_SIZE=0)
            self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),ENVIRONMENT=self.logger_env,PROCESS_INFO=vProcess_info)

    def get_db(self):
        return self.logger_db
    
    def get_db_parameters(self):
        return self.logger_db_param
    
    def get_db_connection(self):
        return self.logger_oracledb_connection
    
    def link_to_bronze_source(self,pBronze_source):
        self.logger_linked_bronze_source = pBronze_source
        self.logger_linked_bronze_config = pBronze_source.get_bronze_config()
        self.logger_env = self.logger_linked_bronze_source.get_bronze_properties().environment
        vSourceProperties = self.logger_linked_bronze_source.get_source_properties()
        self._init_logger()
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=vSourceProperties.name,SRC_ORIGIN_NAME=vSourceProperties.schema,SRC_OBJECT_NAME=vSourceProperties.table)

    def set_logger_properties(self,pSrc_name,pSrc_origin_name,pSrc_object_name,pRequest="",pDuration=0):
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=pSrc_name,SRC_ORIGIN_NAME=pSrc_origin_name,SRC_OBJECT_NAME=pSrc_object_name,REQUEST=pRequest,STAT_TOTAL_DURATION=pDuration)
    
    def get_log_table(self):
        return self.logger_table_name

    def log(self,pAction="SUCCESS",pError=None):
        vAction = pAction
        if pError:
            error_type = type(pError).__name__
            error_message = str(pError)
            # if error message contains warning then change action -> WARNING
            if re.match("WARNING",error_message.upper()):
                VAction = "WARNING"
        else:
            error_type = ''
            error_message = ''
        if self.logger_linked_bronze_source:
            vSourceDurationsStats = self.logger_linked_bronze_source.get_durations_stats()
            vSourceRowsStats = self.logger_linked_bronze_source.get_rows_stats()
            vSourceParquetsStats = self.logger_linked_bronze_source.get_parquets_stats()
            vSourceProperties = self.logger_linked_bronze_source.get_source_properties()

            self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(
                REQUEST=vSourceProperties.request, ACTION=vAction, END_TIME=datetime.now(tz=timezone.utc),ERROR_TYPE=error_type,ERROR_MESSAGE=error_message,
                STAT_ROWS_COUNT=vSourceRowsStats[0], STAT_ROWS_SIZE=vSourceRowsStats[1],
                STAT_SENT_PARQUETS_COUNT=vSourceParquetsStats[0], STAT_SENT_PARQUETS_SIZE=vSourceParquetsStats[1],
                STAT_TOTAL_DURATION=vSourceDurationsStats[0], STAT_FETCH_DURATION=vSourceDurationsStats[1],
                STAT_UPLOAD_PARQUETS_DURATION=vSourceDurationsStats[2], STAT_TEMP_PARQUETS_COUNT=vSourceParquetsStats[2])
        else:
            self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(
                ACTION=vAction, END_TIME=datetime.now(tz=timezone.utc),ERROR_TYPE=error_type,ERROR_MESSAGE=error_message)
            
        self.__insertlog__()

    def __insertlog__(self):
        self.get_db().insert_namedtuple_into_table(self.instance_bronzeloggerproperties,self.logger_table_name)

class BronzeDbManager:
    # object to manage connection to bronze database
    # provide main functions to manage Bronze DB activities
    def __init__(self, pBronze_config:BronzeConfig,pLogger:BronzeLogger):
        self.bronzeDb_Manager_config = pBronze_config
        self.bronzeDbManager_env = self.bronzeDb_Manager_config.get_options().environment
        # Bronze Database name - defined into config.json file
        self.bronzeDb_Manager_database_name = "BRONZE_" + self.bronzeDbManager_env
        self.bronzeDb_Manager_logger = pLogger
        
        self.pre_proc = self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc
        if self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc_args:
            self.pre_proc_args = self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc_args.split(',')
        else:
            self.pre_proc_args = []
        self.post_proc = self.bronzeDb_Manager_config.get_options().PLSQL_post_proc
        if self.bronzeDb_Manager_config.get_options().PLSQL_post_proc_args:
            self.post_proc_args = self.bronzeDb_Manager_config.get_options().PLSQL_post_proc_args.split(',')
        else:
            self.post_proc_args = []
        self.update_lh2_tables_stats_proc = self.bronzeDb_Manager_config.get_options().PLSQL_update_lh2_tables_stats_proc
        if self.bronzeDb_Manager_config.get_options().PLSQL_update_lh2_tables_stats_proc_args:
            self.update_lh2_tables_stats_proc_args = [self.bronzeDbManager_env].append(self.bronzeDb_Manager_config.get_options().PLSQL_update_lh2_tables_stats_proc_args.split(','))
        else:
            self.update_lh2_tables_stats_proc_args = []
        self.lh2_tables_stats = self.bronzeDb_Manager_config.get_options().LH2_TABLES_STATS
        
         # Establish connection to Bronze schema database
        self.bronzeDb_Manager_Database_param = get_parser_config_settings("database")(self.bronzeDb_Manager_config.get_configuration_file(),
                                                                            self.get_bronze_database_name())
        self.bronzeDb_Manager_db:absdb = DBFACTORY.create_instance(self.bronzeDb_Manager_Database_param.dbwrapper,self.bronzeDb_Manager_config.get_configuration_file())

        self.bronzeDb_Manager_db_connection = self.get_db().create_db_connection(self.bronzeDb_Manager_Database_param)
    
    def get_db(self)->absdb:
        return self.bronzeDb_Manager_db
    
    def get_db_parameters(self):
        return self.bronzeDb_Manager_Database_param
    
    def get_db_connection(self):
        return self.bronzeDb_Manager_db_connection
    
    def get_bronze_database_name(self):
        return self.bronzeDb_Manager_database_name
    
    def get_pre_proc(self):
        return (self.pre_proc,self.pre_proc_args)
    
    def get_post_proc(self):
        return (self.post_proc,self.post_proc_args)
    
    def is_bronzetable_exists(self,pTable_name):
        res = False
        if self.get_db_connection():
            res = self.get_db().is_table_exists(pTable_name)
        return res
    
    def get_bronze_lastupdated_row(self,pTable_name,pSrc_date_criteria):
        last_date = None
        if not self.get_db_connection():
            return last_date
        last_date = self.get_db().get_table_max_column(pTable_name, pSrc_date_criteria)
        return last_date
    
    def run_proc(self,pProc_name='',/,*args,pVerbose=None,pProc_exe_context='GLOBAL'):
        vStart = datetime.now()
        vReturn = True
        try:
            if pProc_name and self.get_db():
                vDmbs_output = self.get_db().execute_proc(pProc_name,*args)
                vLog_message = vDmbs_output
                if self.get_db().last_execute_proc_completion():
                    vAction = "SUCCESS"
                    vErr = None
                    vReturn = True
                else:
                    vAction = "ERROR"
                    vErr = Exception("Error during execution of procedure {} with {}".format(pProc_name,args))
                    vReturn = False
        except Exception as vErr:
            vAction = "ERROR calling Procedure {} with {}".format(pProc_name,args)
            vLog_message = 'Oracle DB error :{}'.format(str(vErr))
            vReturn = False
        finally:
            vDuration = datetime.now() - vStart
            self.bronzeDb_Manager_logger.set_logger_properties(self.get_bronze_database_name(),pProc_name,pProc_exe_context,"args:{}".format(str(args)),vDuration)
            
            if pProc_name and self.get_db():
                if pVerbose:
                    pVerbose.log(datetime.now(tz=timezone.utc), "BRONZE_PROC", vAction, log_message=vLog_message)
                self.bronzeDb_Manager_logger.log(pError=vErr, pAction=vAction)
            return vReturn
                 
    def run_pre_proc(self,pVerbose=None):
        return self.run_proc(self.pre_proc,*self.pre_proc_args,pVerbose=pVerbose,pProc_exe_context='GLOBAL')
        
    def run_post_proc(self,pVerbose=None):
       return self.run_proc(self.post_proc,*self.post_proc_args,pVerbose=pVerbose,pProc_exe_context='GLOBAL')
   
    def run_update_lh2_tables_stats_proc(self,pVerbose=None):
        self.run_proc(self.update_lh2_tables_stats_proc,*self.update_lh2_tables_stats_proc_args,pVerbose=pVerbose,pProc_exe_context='GLOBAL')
       
       # Execute a SQL query to fetch activ data from the table "LIST_DATASOURCE_LOADING_..." into a dataframe
        v_cursor = self.get_db_connection.cursor()
        v_param_req = "select * from " + self.lh2_tables_stats + " ORDER BY OWNER,TABLE_NAME"
        v_cursor.execute(v_param_req)
        self.df_tables_stats = pd.DataFrame(v_cursor.fetchall())
        self.df_tables_stats.columns = [x[0] for x in v_cursor.description]
        v_cursor.close()
        # Get distinct bucket names from the column
        v_bronze_buckets_array = self.df_tables_stats['BUCKET'].unique()
        # Create a new DataFrame from the distinct values
        self.df_bronze_buckets_parquets = pd.DataFrame(v_bronze_buckets_array,columns=['BUCKET'])
        # Add a column 'LIST_PARQUETS' with default values (here, empty lists)
        self.df_bronze_buckets_parquets['LIST_PARQUETS'] = [[] for _ in range(len(self.df_bronze_buckets_parquets))]

        v_bronze_bucket_proxy = BronzeBucketProxy(self.bronzeDbManager_env,self.bronzeDb_Manager_config)
        # Iterate over the 'BUCKET' column and change the value of 'LIST_PARQUETS'
        for index, row in self.df_bronze_buckets_parquets.iterrows():
            v_bucket_name = row['BUCKET']
            v_bronze_bucket_proxy.set_bucket_by_name(v_bucket_name)
            v_bucket = v_bronze_bucket_proxy.get_bucket()
            v_bucket_list_objects = v_bucket_name.list_objects()
            self.df_bronze_buckets_parquets.at[index, 'LIST_PARQUETS'] = v_bucket_list_objects

        
   
class BronzeBucketProxy:
    # Class to provide a substitute to manage bronze bucket.
    # add actions to get settings informations to connect to bronze buckets (DEBUG, DEV, STG, PRD)
    # create OCIBUcket object to connect on
    def __init__(self,p_env,p_bronze_config:BronzeConfig):
        self.env = p_env 
        self.bronze_config = p_bronze_config
        if self.env == "DEBUG":
            self.debug = True
            self.oci_settings = get_parser_config_settings("filestorage")(self.bronze_config.get_configuration_file(),"BRONZE_BUCKET_DEBUG")
        else:
            self.debug = False
            self.oci_settings = get_parser_config_settings("filestorage")(self.bronze_config.get_configuration_file(),"BRONZE_BUCKET")
        self.bronze_bucket_settings = None
        self.bucket = None
            
    def set_bucket_by_extension(self,p_bucket_extension):
        # Define bucket name
        v_bucket_name = None
        if not self.debug:
            v_bucket_name = self.bronze_config.get_oci_settings().storage_name + "-" + self.env + "-" + p_bucket_extension
        self.set_bucket_by_name(v_bucket_name)
            
    def set_bucket_by_name(self,p_bucket_name):
        if not self.debug:
            self.bronze_bucket_settings=self.bronze_config.get_oci_settings()._replace(storage_name=p_bucket_name)
        else:
            self.bronze_bucket_settings = type(self.bronze_config.get_oci_settings())._make(self.bronze_config.get_oci_settings())
            
    def connect(self):
        if self.get_bronze_bucket_settings():
            self.bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper,**self.get_bronze_bucket_settings()._asdict())
            return self.bucket
        else:
            self.bucket = None
            return None
    
    def get_bucket(self):
        if not self.bucket:
            self.connect()
        return self.bucket
    
    def get_bronze_bucket_settings(self):
        return self.bronze_bucket_settings
    
    def get_bucket_name(self):
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.storage_name
        else:
            return None
    
    def get_oci_objectstorage_url(self):
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.url
        else:
            return None
    
    def get_oci_adw_credential(self):
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.setting2
        else:
            return None
        
class BronzeSourceBuilder:
    # ENV : environement DEV, STG, PRD
    # config_file : dictionary json config file with connection parameters
    # SRC_TYPE	Type of source	DB SRC_NAME	Database short name
    # EBS SRC_ORIGIN_NAME	Database schema name	exemple :
    # APPS SRC_OBJECT_NAME	Table name	example GL_PERIODS
    # SRC_OBJECT_CONSTRAINT	where clause for filtering data
    # SRC_FLAG_INCR : integrate date in incremental mode, based on date lastupdate
    # SRC_date_where : clause where to filter on records date, might depend on table
    # SRC_date_lastupdate : update last timestamp for data integration
    # FORCE_ENCODE : if UnicodeError during fetch, custom select by converting column of type VARCHAR to a specific CHARSET
    def __init__(self, pSource_properties:SourceProperties, pBronze_config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager,pLogger:BronzeLogger):
        self.bronze_source_properties = pSource_properties
        self.bronze_config = pBronze_config
        self.bronzedb_manager = pBronzeDb_Manager
        self.env = self.bronze_config.get_options().environment

        # Create "tmp" folder to save parquet files
        self.__set_local_workgingdir__(self.bronze_config.get_tempdir())

        self.df_table_content = pd.DataFrame()
        # list of parquet file generated
        self.parquet_file_list = []  # format list : [{"file_name":parquet_file_name1,"source_file":source_file1},{"file_name":parquet_file_name2,"source_file":source_file2},...]
        # list of parquet file to send
        self.parquet_file_list_tosend = [] # # format list : [{"file_name":parquet_file_name1,"source_file":source_file1},{"file_name":parquet_file_name2,"source_file":source_file2},...]
        # send parquet files to OCI bucket
        # Format the timestamp as a string with a specific format (Year-Month-Day)
        self.today = datetime.now(tz=timezone.utc)
        self.year = self.today.strftime("%Y")
        self.month = self.today.strftime("%Y%m")
        self.day = self.today.strftime("%Y%m%d")
        self.bucket_file_path = "" # defined into sub-class
        self.bronze_table = "" # defined into sub-class
        self.bronze_schema = "BRONZE_" + self.env

        self.total_sent_parquets = 0 # Total number of parquet files sent to bucket
        self.total_sent_parquets_size = 0 # total size of parquet files
        self.total_temp_parquets = 0 # total of temporary parquets created when fetching data, before merging for no incremental table
        self.total_imported_rows = 0  # Total number of rows imported from the source table
        self.total_imported_rows_size = 0 # size of importted rows from source
        self.start = datetime.now()
        self.fetch_start = None
        self.upload_parquets_start = None
        self.total_duration = datetime.now() - datetime.now() # duration of requests
        self.fetch_duration = datetime.now() - datetime.now() # duration to ftech data and create local parquet files
        self.upload_parquets_duration = datetime.now() - datetime.now() # duration to send parquet files to OCI
        # if debug = True then we use a dedicated bucket to store parquet files. Bucket name identified into json config
        self.debug = self.bronze_config.isdebugmode()

        # set bronze bucket settings 
        self.bronze_bucket_proxy = BronzeBucketProxy(self.env,self.bronze_config)
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.bronze_source_properties.name)
         
        # To be set into subclass
        self.source_db = None
        self.source_db_connection = None

        # For DB source, build select request
        self.where = ''
        self.db_execute_bind = []
        if self.bronze_source_properties.table_constraint != None:
            self.where += self.bronze_source_properties.table_constraint
        if self.bronze_source_properties.date_criteria != None and self.bronze_source_properties.last_update != None:
            if self.where != "":
                self.where += " AND "
            else:
                self.where += " WHERE "
            self.where += self.bronze_source_properties.date_criteria + " > :1"
            self.db_execute_bind = [self.bronze_source_properties.last_update]
        self.request = ""

        # Set Bronze table settings : table name, bucket path to add parquet files, get index to restart parquet files interation
        self.__set_bronze_table_settings__()
        
        # setup logger to log events errors and success into LOG_ table
        self.logger = pLogger
        self.logger.link_to_bronze_source(self)

    def __init_bronze_bucket_settings__(self):
        self.__set_bronze_bucket_settings__(bucket_extension=self.bronze_source_properties.name)
        
    def __set_local_workgingdir__(self, path):
        # Create a temporary directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
        self.local_workingdir = path

    def __update_fetch_row_stats__(self):
        self.total_imported_rows += len(self.df_table_content)
        self.total_imported_rows_size += int(self.df_table_content.memory_usage(deep=True).sum())
        if self.fetch_start:
            self.fetch_duration = datetime.now() - self.fetch_start

    def __update_sent_parquets_stats(self):
        self.total_temp_parquets = len(self.parquet_file_list)
        self.total_sent_parquets = len(self.parquet_file_list_tosend)
        for p in self.parquet_file_list_tosend:
            source_file = p["source_file"]
            self.total_sent_parquets_size += os.path.getsize(source_file)
            # Deleting temp file
            os.remove(source_file)
        if self.upload_parquets_start:
            self.upload_parquets_duration = datetime.now() - self.upload_parquets_start
        else:
            self.upload_parquets_start = datetime.now()

    def __add_integration_date_column__(self):
        # Adding column into every parquet file with System date to log integration datetime
        integration_time = datetime.now(tz=timezone.utc)
        self.df_table_content["fetch_date"] = integration_time

    def __get_last_parquet_idx_in_bucket__(self):
        # check if other parquet files already exist into the same bucket folder
        # get the last id parquet number
        # to avoid remplace existing parquet files
        idx = 0
        try:
            #bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper,**self.get_bronze_bucket_settings()._asdict())
            bucket = self.bronze_bucket_proxy.get_bucket()
            what_to_search = self.bucket_file_path+self.parquet_file_name_template
            list_buckets_files = [obj.name for obj in bucket.list_objects(what_to_search)]
            if list_buckets_files:
                max_file = max(list_buckets_files)
                # eliminate file extension
                begin_file_name = max_file.split('.')[0]
                # span() returns a tuple containing the start-, and end positions of the match.
                pos = re.search(what_to_search, begin_file_name).span()[1]
                idx = int(begin_file_name[pos:])
        except:
            idx = 0
        return idx

    def __set_bronze_table_settings__(self):
        #define bronze table name, bucket path to add parquet files, get index to restart parquet files interation
        pass
    
    def __set_bronze_bucket_settings__(self,bucket_extension):
        # Define bucket name
        if not self.debug:
            self.bronze_bucket_settings=self.bronze_config.get_oci_settings()._replace(storage_name=self.bronze_config.get_oci_settings().storage_name + "-" + self.env + "-" + bucket_extension)
        else:
            self.bronze_bucket_settings = type(self.bronze_config.get_oci_settings())._make(self.bronze_config.get_oci_settings())
        
        # Set information needed to create external tables into bucket
        self.bucketname = self.bronze_bucket_settings.storage_name
        self.oci_objectstorage_url = self.bronze_bucket_settings.url
        self.oci_adw_credential = self.bronze_bucket_settings.setting2
        
    def __clean_temporary_parquet_files__(self):
        # clean temporay local parquet files (some could remain from previous failure process
        merged_parquet_file_name = self.parquet_file_name_template + PARQUET_FILE_EXTENSION
        merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
        list_files = list_template_files(os.path.splitext(merged_parquet_file)[0])
        for f in list_files:
            os.remove(f)

    def __create_parquet_file__(self,verbose=None):
        # Parquet file si created locally into local_workingdir
        # Define the path and filename for the Parquet file, id on PARQUET_IDX_DIGITS digits
        self.parquet_file_id += 1

        parquet_file_name= '{0}{1}{2}'.format(self.parquet_file_name_template,str(self.parquet_file_id).zfill(PARQUET_IDX_DIGITS),PARQUET_FILE_EXTENSION)
        source_file = os.path.join(self.local_workingdir, parquet_file_name)

        try:
            # Create parquet file only if number of dataframe row > 0
            if len(self.df_table_content.index) > 0:
                #adding date of integration - new column into df
                self.__add_integration_date_column__()
                # Write the DataFrame to a Parquet file with the specified compression
                message = "Creating parquet file {0} into local folder {1}".format(parquet_file_name,source_file)
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc),"CREATE_PARQUET","START",log_message=message)
                self.df_table_content.to_parquet(source_file, engine='pyarrow', compression='None')
                self.parquet_file_list.append({"file_name": parquet_file_name, "source_file": source_file})
                #print("parquet created")
                return True
            else:
                message = "No row for {}".format(parquet_file_name)
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", "START", log_message=message)
                return False
        except OSError as err:
            vError = "ERROR Creating parquet file {}".format(parquet_file_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", vError,log_message='OS Error : '.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            return False
        except Exception as err:
            vError = "ERROR Creating parquet file {}".format(parquet_file_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", vError,log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            # continue can only be used within a loop, so we use pass instead
            return False

    def __custom_select_from_source__(self):
        # create select request from source
        # if need to encode columns, cast columns on select request
        if self.source_db:
            full_table_name = self.source_db.get_full_table_name(self.bronze_source_properties.schema,self.bronze_source_properties.table)
            if not self.bronze_source_properties.force_encode:
                self.request = "select * from " + full_table_name 
            else:
                if not self.source_db_connection:
                    raise Exception("Error no DB connection")
                (could_custom_select,custom_select_result) = self.source_db.create_select_encode_from_table(full_table_name, self.bronze_source_properties.force_encode)
                if could_custom_select:
                    self.request = custom_select_result
                else:
                    raise Exception(custom_select_result)
            self.request = self.request + " " + self.where 

    def __sync_bronze_table__(self,verbose=None):
        table = self.bronze_table
        try:
            if not self.get_bronzedb_manager().get_db_connection():
                raise Exception("Error no DB connection")
            cursor = self.get_bronzedb_manager().get_db_connection().cursor()

            request = 'BEGIN DBMS_CLOUD.SYNC_EXTERNAL_PART_TABLE(table_name =>\'' + table + '\'); END;'

            # Execute a PL/SQL to synchronize Oracle partionned external table
            if verbose:
                message = "Synchronizing external partionned table {}".format(table)
                verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE", "SYNC", log_message=message)
            cursor.execute(request)
            cursor.close()
            return True

        except oracledb.Error as err:
            vError = "ERROR Synchronizing table {}".format(table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SYNC_TABLE", vError, log_message='Oracle DB error :{}'.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            return False
        except Exception as err:
            vError = "ERROR Synchronizing table {}".format(table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SYNC_TABLE", vError,log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False

    def __create_bronze_table__(self,verbose=None):
        vTable = self.bronze_table
        try:
            if not self.get_bronzedb_manager().get_db_connection():
                raise Exception("Error no DB connection")
            cursor = self.get_bronzedb_manager().get_db_connection().cursor()
            # Dropping table before recreating
            #drop = 'BEGIN EXECUTE IMMEDIATE \'DROP TABLE ' + vTable + '\'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;'
            if verbose:
                message = "Dropping table {}.{} ".format(self.get_bronzedb_manager().get_db_parameters().p_username,vTable)
                verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=message)
            #cursor.execute(drop)
            vResult_run_proc = self.get_bronzedb_manager().run_proc('ADMIN.DROP_TABLE',*[self.get_bronzedb_manager().get_db_parameters().p_username,vTable],pVerbose=verbose,pProc_exe_context=vTable)
            if not vResult_run_proc:
                raise Exception("ERROR dropping table {}".format(vTable))
            
            # Create external part table parsing parquet files from bucket root (for incremental mode)
            if self.bronze_source_properties.incremental:
                # Create external part table parsing parquet files from bucket root (for incremental mode)
                root_path = self.bucket_file_path.split("/")[0]+"/"
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.bronze_bucket_proxy.get_oci_adw_credential() + '\', file_uri_list =>\'' + self.bronze_bucket_proxy.get_oci_objectstorage_url() + self.bronze_bucket_proxy.get_bucket_name() + '/o/'+root_path+'*' + self.parquet_file_name_template + '*.parquet\', format => \'{"type":"parquet", "schema": "first","partition_columns":[{"name":"fetch_year","type":"varchar2(100)"},{"name":"fetch_month","type":"varchar2(100)"},{"name":"fetch_day","type":"varchar2(100)"}]}\'); END;'
                # create += 'EXECUTE IMMEDIATE '+ '\'CREATE INDEX fetch_date ON ' + table + '(fetch_year,fetch_month,fetch_date)\'; END;'
                # not supported for external table
            else:
                # Create external table linked to ONE parquet file (for non incremental mode)
                root_path = self.bucket_file_path
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.bronze_bucket_proxy.get_oci_adw_credential() + '\', file_uri_list =>\'' + self.bronze_bucket_proxy.get_oci_objectstorage_url() + self.bronze_bucket_proxy.get_bucket_name() + '/o/' + root_path + self.parquet_file_name_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
            if verbose:
                message = "Creating table {} : {}".format(vTable,create)
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "START", log_message=message)
            cursor.execute(create)
            # Alter column type from BINARY_DOUBLE to NUMBER
            if verbose:
                message = "Altering table columns type {}.{}".format(self.get_bronzedb_manager().get_db_parameters().p_username,vTable)
                verbose.log(datetime.now(tz=timezone.utc), "ALTER_TABLE", "START", log_message=message)
            vResult_run_proc = self.get_bronzedb_manager().run_proc('ADMIN.ALTER_TABLE_COLUMN_TYPE',*[self.get_bronzedb_manager().get_db_parameters().p_username,vTable,'BINARY_DOUBLE','NUMBER(38,10)'],pVerbose=verbose,pProc_exe_context=vTable)
            if not vResult_run_proc:
                raise Exception("ERROR altering table columns type {}.{}".format(self.get_bronzedb_manager().get_db_parameters().p_username,vTable))
            cursor.close()
            return True

        except oracledb.Error as err:
            vError= "ERROR Creating table {}".format(vTable)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError,log_message='Oracle DB error : {}'.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            return False
        except Exception as err:
            vError = "ERROR Creating table {}".format(vTable)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError,log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False

    def set_bronzedb_connection(self,pBronzeDbManager:BronzeDbManager):
        self.bronzedb_manager = pBronzeDbManager
        
    def update_total_duration(self):
        self.total_duration = datetime.now() - self.start 

    def get_bronze_config(self):
        return self.bronze_config

    def get_bronzedb_manager(self):
        return self.bronzedb_manager
    
    def get_post_procedure_parameters(self):
        if self.bronze_source_properties.bronze_post_proc:
            return (self.bronze_source_properties.bronze_post_proc,self.bronze_source_properties.bronze_post_proc_args)
        else:
            return None
    
    def get_rows_stats(self):
        return (self.total_imported_rows, self.total_imported_rows_size)

    def get_durations_stats(self):
        return (self.total_duration,self.fetch_duration,self.upload_parquets_duration)

    def get_parquets_stats(self):
        return (self.total_sent_parquets, self.total_sent_parquets_size,self.total_temp_parquets)

    def get_source_properties(self):
        return self.bronze_source_properties

    def get_bronze_properties(self):
        return BronzeProperties(self.env,self.bronze_schema,self.bronze_table,self.bronze_bucket_proxy.get_bucket_name(),self.bucket_file_path,self.parquet_file_name_template)

    def get_logger(self):
        return self.logger

    def get_bronze_lastupdated_row(self):
        return self.get_bronzedb_manager().get_bronze_lastupdated_row(self.bronze_table, self.bronze_source_properties.date_criteria)

    def get_bronze_bucket_settings(self):
        return self.bronze_bucket_settings
    
    def send_parquet_files_to_oci(self,verbose=None):
        self.upload_parquets_start = datetime.now()
        self.__update_sent_parquets_stats()

        if not self.parquet_file_list:
            vError = "WARNING, No parquet files to upload"
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message="")
            self.logger.log(pError=Exception(vError), pAction=vError)
            return False
        self.parquet_file_list_tosend = []
        if not self.bronze_source_properties.incremental:
            # if not incremental mode, merging parquet files into one, before sending
            merged_parquet_file_name = self.parquet_file_name_template+PARQUET_FILE_EXTENSION
            merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
            # Use 1st alternative to merge parquet files
            #parquet_files_tomerge = [p["source_file"] for p in self.parquet_file_list]
            #merge_parquet_files(parquet_files_tomerge,merged_parquet_file,removesource=True,self.verbose)
            #########################
            # Use 2nd alternative to merge parquet file based on same root names - based on duckdb
            duckdb_db = DBFACTORY.create_instance(self.bronze_config.get_duckdb_settings().dbwrapper,self.bronze_config.get_configuration_file())
            duckdb_db_connection = duckdb_db.create_db_connection(self.bronze_config.get_duckdb_settings())
            merge_template_parquet_files(os.path.splitext(merged_parquet_file)[0],duckdb_db_connection,True,verbose)

            self.parquet_file_list_tosend = [{"source_file":merged_parquet_file,"file_name":merged_parquet_file_name}]
        else:
            self.parquet_file_list_tosend = self.parquet_file_list

        try:
            #bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper,**self.get_bronze_bucket_settings()._asdict())
            bucket = self.bronze_bucket_proxy.get_bucket()
        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR create access to bucket {0}".format(self.bronze_bucket_proxy.get_bucket_name())
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "BUCKET_ACCESS", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False
        try:    
            for p in self.parquet_file_list_tosend:
                # Sending parquet files
                    bucket_file_name = self.bucket_file_path +  p["file_name"]
                    source_file = p["source_file"]

                    message = "Uploading parquet from {0} into bucket {1}, {2}".format(source_file,self.bronze_bucket_proxy.get_bucket_name(),bucket_file_name)
                    if verbose:
                        verbose.log(datetime.now(tz=timezone.utc),"UPLOAD_PARQUET","START",log_message=message)
                    bucket.put_file(bucket_file_name, source_file)
        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR Uplaoding parquet file {0} into bucket {1}, {2}".format(source_file,self.bronze_bucket_proxy.get_bucket_name(),bucket_file_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False
        self.__update_sent_parquets_stats()
        return True

    def update_bronze_schema(self,verbose):
        try:
            message = "Update Bronze schema {} : Incremental {}".format(self.bronze_table, bool(self.bronze_source_properties.incremental))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "START", log_message=message)
            if not self.parquet_file_list:
                message = "No parquet files No update of Bronze needed"
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "END", log_message=message)
                res = True
            elif self.bronze_source_properties.incremental and self.get_bronzedb_manager().is_bronzetable_exists(self.bronze_table):
                # if incremental mode, if table exists, update (synchronize) external part table
                res = self.__sync_bronze_table__(verbose)
            else:
                #if incremental mode,if table not exists,
                #create external part table linked to "parquet_file_name_template"xxx.parquets files from bucket root
                # if no incremental mode, drop existing table and recreate table linked to uploaded parquet file
                res = self.__create_bronze_table__(verbose)
            return res
        except Exception as err:
            vError = "ERROR Updating bronze schema, table : {}".format(self.bronze_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPDATE_BRONZE_SCHEMA", vError,log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False

    def pre_fetch_source(self,verbose=None):
        # TO BE EXECUTED before fetch method
        self.fetch_start = datetime.now()
        #delete old temporary parquet files
        self.__clean_temporary_parquet_files__()

    def fetch_source(self,verbose=None):
        # Request data from source
        # and generate local parquet files
        pass

class BronzeGenerator:
    def __init__(self, pBronzeSourceBuilder:BronzeSourceBuilder, pBronzeExploit:BronzeExploit,pLogger:BronzeLogger=None):
        self.__bronzesourcebuilder__ = pBronzeSourceBuilder
        self.__bronzeexploit__ = pBronzeExploit
        self.__logger__ = pLogger

    def generate(self,verbose=None):
        while True:
            generate_result = False
            # 1 Fetch data from source
            self.__bronzesourcebuilder__.pre_fetch_source(verbose)
            if not self.__bronzesourcebuilder__.fetch_source(verbose):
                break

            # 2 Upload parquets files to bucket
            if not self.__bronzesourcebuilder__.send_parquet_files_to_oci(verbose):
                break

            # 3 - Create/Update external table into Autonomous
            if not self.__bronzesourcebuilder__.update_bronze_schema(verbose):
                break

            # 4 Update "Last_update" for incremental table integration
            vSourceProperties = self.__bronzesourcebuilder__.get_source_properties()
            if vSourceProperties.incremental:
                last_date = self.__bronzesourcebuilder__.get_bronze_lastupdated_row()
                #print(last_date, type(last_date))

                if not self.__bronzeexploit__.update_exploit(vSourceProperties,"SRC_DATE_LASTUPDATE", last_date):
                    break
            vPost_proc_param = self.__bronzesourcebuilder__.get_post_procedure_parameters()
            if vPost_proc_param:
                vBronze_table = self.__bronzesourcebuilder__.get_bronze_properties.table
                vResult_post_proc = self.__bronzesourcebuilder__.get_bronzedb_manager().run_proc(vPost_proc_param[0],*vPost_proc_param[1],pVerbose=verbose,pProc_exe_context=vBronze_table)
                if not vResult_post_proc:
                    break
            
            generate_result = True
            break
        self.__bronzesourcebuilder__.update_total_duration()
        if generate_result:
            if verbose:
                #print(vSourceProperties)
                message = "Integrating {3} rows from {0} {1} {2} in {4}".format(vSourceProperties.name, vSourceProperties.schema,
                                                                         vSourceProperties.table, self.__bronzesourcebuilder__.get_rows_stats()[0],self.__bronzesourcebuilder__.get_durations_stats()[0])
                verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "END", log_message=message)
            if self.__logger__:
                self.__logger__.log()
        return generate_result

