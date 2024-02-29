import socket
import os
import os.path

import pandas as pd

from nlsoci.oci_bucket import *
from nlstools.config_settings import *
from nlstools.tool_kits import *
from nlsdb.dbwrapper_factory import *

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

class BronzeConfig():
    #Define configuration envrionment parameters to execute.
    #Mainly extract parameters from json configuration_file
    #Provide a generic method to get an oracledb connection, depending on database options defined into json

    def __init__(self,configuration_file):
        self.configuration_file = configuration_file
        self.options = get_parser_config_settings("options")(self.configuration_file,"options")
        #self.oracledb_settings = get_parser_config_settings("oracledb_settings")(self.configuration_file,"oracledb_settings")
        self.duckdb_settings = get_parser_config_settings("duckdb_settings")(self.configuration_file,"duckdb_settings")
        self.oci_settings = get_parser_config_settings("oci")(self.configuration_file,"oci")
        #self.odbcdb_settings = get_parser_config_settings("odbcdb_settings")(self.configuration_file,"odbcdb_settings")

        if self.options.db_arraysize.isdigit():
            global DB_ARRAYSIZE
            DB_ARRAYSIZE = eval(self.options.db_arraysize)

        if not self.options.sql_readmode:
            global SQL_READMODE
            SQL_READMODE = self.options.sql_readmode

        if self.options.environment == "DEBUG":
            self.debug = True
        else:
            self.debug = False

        #Init Oracledb envrionment - only once
        #db_oracle = DBFACTORY.create_instance("OracleDbWrapper",self.configuration_file)
        #db_oracle.set_db_settings(self.oracledb_settings)

        if self.options.rootdir != '':
            os.chdir(self.options.rootdir)
            self.rootdir = os.getcwd()
        else:
            self.rootdir = ''

        # Create a temporary directory if it doesn't exist
        if not os.path.exists(self.options.tempdir):
            os.makedirs(self.options.tempdir)
        self.tempdir = os.path.join(self.rootdir,self.options.tempdir)

        #create log file based on : lgofile_name_template + _ + Now timestamp + _ + processus PID + . + extension
        # to have an unique log file name is several process are running in parallel
        vSplitLogFile = os.path.splitext(self.get_options().verboselogfile)
        vPid = os.getpid()
        vTimestamp = round(datetime.now(tz=timezone.utc).timestamp())
        vLogFile = '{}_{}_{}{}'.format(vSplitLogFile[0],vTimestamp,vPid,vSplitLogFile[1])
        self.verboselogfile = os.path.join(self.get_tempdir(),vLogFile)

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

class BronzeExploit:
    # Iterator object for list of sources to be imported into Bronze
    # Define metohd to update src_dat_lastupdate for table with incremental integration

    def __init__(self,br_config,verbose=None,**optional_args):
        self.idx = 0
        self.verbose = verbose
        self.bronze_config = br_config
        self.exploit_db_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),"exploit")
        self.db = DBFACTORY.create_instance(self.exploit_db_param.dbwrapper,self.bronze_config.get_configuration_file())
        self.oracledb_connection = self.db.create_db_connection(self.exploit_db_param)

        self.exploit_loading_table = self.bronze_config.get_options().datasource_load_tablename_prefix + self.bronze_config.get_options().environment
        cursor = self.oracledb_connection.cursor()

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
            proc_out_duplicate_table = cursor.var(bool)
            proc_out_createlh2_datasource = cursor.var(str)

            exploit_running_loading_table_fullname = self.exploit_db_param.p_username +'.' + self.exploit_running_loading_table

            message = "Create running Exploit table  {}".format(self.exploit_running_loading_table)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=message)

            if optional_args[EXPLOIT_ARG_RELOAD_ON_ERROR_INTERVAL] and optional_args[EXPLOIT_ARG_LOG_TABLE] :
                cursor.callproc('ADMIN.DUPLICATE_TABLE', [self.exploit_db_param.p_username, self.exploit_loading_table,self.exploit_db_param.p_username,self.exploit_running_loading_table,False,proc_out_duplicate_table])

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
                cursor.callproc('CREATE_LH2_DATASOURCE_LOADING_ON_ERROR',[self.exploit_loading_table,self.exploit_running_loading_table,vLog_table,vInterval_start,vInterval_end,proc_out_createlh2_datasource])
                self.oracledb_connection.commit()
            else:
                cursor.callproc('ADMIN.DUPLICATE_TABLE',[self.exploit_db_param.p_username, self.exploit_loading_table,self.exploit_db_param.p_username,self.exploit_running_loading_table, True, proc_out_duplicate_table])
                self.oracledb_connection.commit()
            if not proc_out_duplicate_table.getvalue():
                message = "ERROR, Create running Exploit table  {}".format(self.exploit_running_loading_table)
                if self.verbose:
                    self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "ERROR", log_message=message)
                raise(Exception(message))
        else:
            self.exploit_running_loading_table = optional_args[EXPLOIT_ARG_LOADING_TABLE]
            message = "Using running Exploit table  {}".format(self.exploit_running_loading_table)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=message)

        # Execute a SQL query to fetch activ data from the table "LIST_DATASOURCE_LOADING_..." into a dataframe
        param_req = "select * from " + self.exploit_running_loading_table + " where SRC_FLAG_ACTIV = 1 ORDER BY SRC_TYPE,SRC_NAME,SRC_OBJECT_NAME"
        cursor.execute(param_req)
        self.df_param = pd.DataFrame(cursor.fetchall())
        self.df_param.columns = [x[0] for x in cursor.description]
        cursor.close()

    def __del__(self):
        if not self.not_drop_running_loading_table:
            cursor = self.oracledb_connection.cursor()
            #message = "Dropping Exploit table {}".format(self.exploit_running_loading_table)
            #if self.verbose:
            #    self.verbose.log(datetime.now(tz=timezone.utc), "END_EXPLOIT", "DROP", log_message=message)
            cursor.callproc('ADMIN.DROP_TABLE', [self.exploit_db_param.p_username,self.exploit_running_loading_table])
            cursor.close()
    def __iter__(self):
        return self

    def __next__(self):
        try:
            items = [self.df_param.iloc[self.idx,i] for i in range(len(self.df_param.columns))]
        except IndexError:
            raise StopIteration()
        self.idx += 1
        return items

    def get_loading_tables(self):
        return (self.exploit_loading_table,self.exploit_running_loading_table)

    def update_exploit(self,src_name,src_origin_name,src_object_name,column_name, value):
        request = ""
        try:
            cursor = self.oracledb_connection.cursor()
            request = "UPDATE " + self.exploit_loading_table + " SET "+column_name+" = :1 WHERE SRC_NAME = :2 AND SRC_ORIGIN_NAME = :3 AND SRC_OBJECT_NAME = :4"

            # update last date or creation date (depends on table)
            message = "Updating {} = {} on table {}".format(column_name,value,self.exploit_loading_table)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "SET_LASTUPDATE", "START", log_message=message,
                            log_request=request)
            bindvars = (value,src_name,src_origin_name,src_object_name)
            cursor.execute(request,bindvars)
            self.oracledb_connection.commit()
            cursor.close()
            return True
        except oracledb.Error as err:
            vError = "ERROR {} with values {}".format(request,bindvars)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", vError,log_message='Oracle DB error : {}'.format(str(err)))
            self.logger.log(error=err, action=vError)
            return False
        except Exception as err:
            vError = "ERROR {} with values {}".format(request, bindvars)
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", vError,str(err))
            self.logger.log(error=err, action=vError)
            return False

    def __str__(self):
        return f"BronzeExploit: exploit_running_loading_table={self.exploit_running_loading_table}, df_param={self.df_param}"

class BronzeLogger():
    def __init__(self, bronze_config,verbose=None):
        self.bronze_source = None
        self.bronze_config = bronze_config
        self.verbose = verbose
        self.cursor = None
        self.oracledb_connection = None
        if self.bronze_config:
            self.env = bronze_config.get_options().environment
            self.table_name = self.bronze_config.get_options().log_table_prefix + self.bronze_config.get_options().environment
        self._init_logger()

    def __del__(self):
        if self.cursor:
            self.cursor.close()

    def _init_logger(self):
        if self.bronze_config:
            self.__del__()
            # Establish a connection to EXPLOIT schema to log message
            self.logger_db_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),
                                                                           self.bronze_config.get_options().logger_db)
            self.db = DBFACTORY.create_instance(self.logger_db_param.dbwrapper,self.bronze_config.get_configuration_file())

            self.oracledb_connection = self.db.create_db_connection(self.logger_db_param)
            self.cursor = self.oracledb_connection.cursor()

            self.BronzeLoggerProperties = self.db.create_namedtuple_from_table('BronzeLoggerProperties',self.table_name)
            #self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),END_TIME=None, ENVIRONMENT=self.env,ACTION='',SRC_NAME='',SRC_ORIGIN_NAME='',SRC_OBJECT_NAME='',REQUEST='',ERROR_TYPE='',ERROR_MESSAGE='',STAT_ROWS_COUNT=0,STAT_ROWS_SIZE=0,STAT_TOTAL_DURATION=0,STAT_FETCH_DURATION=0,STAT_UPLOAD_PARQUETS_DURATION=0,STAT_SENT_PARQUETS_COUNT=0,STAT_SENT_PARQUETS_SIZE=0)
            self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),ENVIRONMENT=self.env)

    def link_to_bronze_source(self,br_source):
        self.bronze_source = br_source
        self.bronze_config = br_source.get_bronze_config()
        self.env = self.bronze_source.get_bronze_properties().environment
        vSourceProperties = self.bronze_source.get_source_properties()
        self._init_logger()
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=vSourceProperties.name,SRC_ORIGIN_NAME=vSourceProperties.schema,SRC_OBJECT_NAME=vSourceProperties.table)

    def get_log_table(self):
        return self.table_name

    def log(self,action="SUCCESS",error=None):
        if error:
            error_type = type(error).__name__
            error_message = str(error)
        else:
            error_type = ''
            error_message = ''
        vSourceDurationsStats = self.bronze_source.get_durations_stats()
        vSourceRowsStats = self.bronze_source.get_rows_stats()
        vSourceParquetsStats = self.bronze_source.get_parquets_stats()
        vSourceProperties = self.bronze_source.get_source_properties()

        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(
            REQUEST=vSourceProperties.request, ACTION=action, END_TIME=datetime.now(tz=timezone.utc),ERROR_TYPE=error_type,ERROR_MESSAGE=error_message,
            STAT_ROWS_COUNT=vSourceRowsStats[0], STAT_ROWS_SIZE=vSourceRowsStats[1],
            STAT_SENT_PARQUETS_COUNT=vSourceParquetsStats[0], STAT_SENT_PARQUETS_SIZE=vSourceParquetsStats[1],
            STAT_TOTAL_DURATION=vSourceDurationsStats[0], STAT_FETCH_DURATION=vSourceDurationsStats[1],
            STAT_UPLOAD_PARQUETS_DURATION=vSourceDurationsStats[2], STAT_TEMP_PARQUETS_COUNT=vSourceParquetsStats[2])
        self.__insertlog__()

    def __insertlog__(self):
        self.db.insert_namedtuple_into_table(self.instance_bronzeloggerproperties,self.table_name)

SourceProperties = namedtuple('SourceProperties',['type','name','schema','table','request','incremental','lastupdate'])
BronzeProperties = namedtuple('BronzeProperties',['environment','schema','table','bucket','bucket_filepath','parquet_template'])

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
    def __init__(self, br_config, src_type, src_name, src_origin_name, src_table_name, src_table_where,
                 src_flag_incr, src_date_criteria, src_date_lastupdate,force_encode,logger):
        self.bronze_config = br_config
        self.env = self.bronze_config.get_options().environment
        self.src_type = src_type
        self.src_name = src_name
        self.src_schema = src_origin_name
        self.src_table = src_table_name
        self.src_table_whereclause = src_table_where
        self.src_flag_incr = src_flag_incr
        self.src_date_criteria = src_date_criteria
        self.src_date_lastupdate = src_date_lastupdate
        self.force_encode = force_encode

        # Create "tmp" folder to save parquet files
        self.__set_local_workgingdir__(self.bronze_config.get_tempdir())

        self.parquet_file_name_template = self.src_name + "_" + self.src_table.replace(" ", "_")
        self.parquet_file_id = 0
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

        # Define bucket name
        if not self.debug:
            self.bucketname = self.bronze_config.get_oci_settings().bucket_prefix_name + "-" + self.env + "-" + self.src_name
        else:
            self.bucketname = self.bronze_config.get_oci_settings().bucket_debug

        # To be set into subclass
        self.source_db = None
        self.source_db_connection = None

        # For DB source, build select request
        self.where = ''
        self.db_execute_bind = []
        if self.src_table_whereclause != None:
            self.where += self.src_table_whereclause
        if self.src_date_criteria != None and self.src_date_lastupdate != None:
            if self.where != "":
                self.where += " AND "
            else:
                self.where += " WHERE "
            self.where += self.src_date_criteria + " > :1"
            self.db_execute_bind = [self.src_date_lastupdate]
        self.request = ""

        # setup logger to log events errors and success into LOG_ table
        self.logger = logger
        self.logger.link_to_bronze_source(self)

        # Establish connection to Bronze schema database
        self.bronze_database_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),
                                                                            self.bronze_schema)
        self.bronze_db = DBFACTORY.create_instance(self.bronze_database_param.dbwrapper,self.bronze_config.get_configuration_file())

        self.bronze_db_connection = self.bronze_db.create_db_connection(self.bronze_database_param)


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
            # define your OCI config
            oci_config_path = self.bronze_config.get_oci_settings().config_path
            oci_config_profile = self.bronze_config.get_oci_settings().profile
            bucket = OCIBucket(self.bucketname, file_location=oci_config_path, oci_profile=oci_config_profile)

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
            self.logger.log(error=err, action=vError)
            return False
        except Exception as err:
            vError = "ERROR Creating parquet file {}".format(parquet_file_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", vError,log_message=str(err))
            self.logger.log(error=err, action=vError)
            # continue can only be used within a loop, so we use pass instead
            return False

    def __is_bronzetable_exists__(self,table_name):
        res = False
        if not self.bronze_db_connection:
            return res
        res = self.bronze_db.is_table_exists(table_name)
        return res

    def __custom_select_from_source__(self):
        if self.source_db:
            full_table_name = self.source_db.get_full_table_name(self.src_schema,self.src_table)
            if not self.force_encode:
                self.request = "select * from " + full_table_name
            else:
                if not self.source_db_connection:
                    raise
                self.request = self.source_db.create_select_encode_from_table(full_table_name, self.force_encode)
            self.request += " " + self.where

    def __sync_bronze_table__(self,verbose=None):
        table = self.bronze_table
        try:
            if not self.bronze_db_connection:
                raise
            cursor = self.bronze_db_connection.cursor()

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
            self.logger.log(error=err, action=vError)
            return False
        except Exception as err:
            vError = "ERROR Synchronizing table {}".format(table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SYNC_TABLE", vError,log_message=str(err))
            self.logger.log(error=err, action=vError)
            return False

    def __create_bronze_table__(self,verbose=None):
        vTable = self.bronze_table
        try:
            if not self.bronze_db_connection:
                raise
            cursor = self.bronze_db_connection.cursor()
            # Dropping table before recreating
            #drop = 'BEGIN EXECUTE IMMEDIATE \'DROP TABLE ' + vTable + '\'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;'
            if verbose:
                message = "Dropping table {}.{} ".format(self.bronze_database_param.p_username,vTable)
                verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=message)
            #cursor.execute(drop)
            cursor.callproc('ADMIN.DROP_TABLE',[self.bronze_database_param.p_username,vTable])

            # Create external part table parsing parquet files from bucket root (for incremental mode)
            if self.src_flag_incr:
                # Create external part table parsing parquet files from bucket root (for incremental mode)
                root_path = self.bucket_file_path.split("/")[0]+"/"
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/'+root_path+'*' + self.parquet_file_name_template + '*.parquet\', format => \'{"type":"parquet", "schema": "first","partition_columns":[{"name":"fetch_year","type":"varchar2(100)"},{"name":"fetch_month","type":"varchar2(100)"},{"name":"fetch_day","type":"varchar2(100)"}]}\'); END;'
                # create += 'EXECUTE IMMEDIATE '+ '\'CREATE INDEX fetch_date ON ' + table + '(fetch_year,fetch_month,fetch_date)\'; END;'
                # not supported for external table
            else:
                # Create external table linked to ONE parquet file (for non incremental mode)
                root_path = self.bucket_file_path
                #create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/'+root_path+ self.parquet_file_name_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://frysfb5gvbrr.objectstorage.eu-frankfurt-1.oci.customer-oci.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/' + root_path + self.parquet_file_name_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
            if verbose:
                message = "Creating table {} : {}".format(vTable,create)
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "START", log_message=message)
            cursor.execute(create)
            # Alter column type from BINARY_DOUBLE to NUMBER
            #alter_table = 'BEGIN ADMIN.ALTER_TABLE_COLUMN_TYPE(\'' + self.bronze_database_param.p_username + '\',\'' + vTable + '\',\'BINARY_DOUBLE\',\'NUMBER\'); END;'
            if verbose:
                message = "Altering table columns type {}.{}".format(self.bronze_database_param.p_username,vTable)
                verbose.log(datetime.now(tz=timezone.utc), "ALTER_TABLE", "START", log_message=message)
            #cursor.execute(alter_table)
            cursor.callproc('ADMIN.ALTER_TABLE_COLUMN_TYPE',[self.bronze_database_param.p_username,vTable,'BINARY_DOUBLE','NUMBER'])
            cursor.close()
            return True

        except oracledb.Error as err:
            vError= "ERROR Creating table {}".format(vTable)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError,log_message='Oracle DB error : {}'.format(str(err)))
            self.logger.log(error=err, action=vError)
            return False
        except Exception as err:
            vError = "ERROR Creating table {}".format(vTable)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError,log_message=str(err))
            self.logger.log(error=err, action=vError)
            return False

    def update_total_duration(self):
        self.total_duration = datetime.now() - self.start

    def get_bronze_config(self):
        return self.bronze_config

    def get_rows_stats(self):
        return (self.total_imported_rows, self.total_imported_rows_size)

    def get_durations_stats(self):
        return (self.total_duration,self.fetch_duration,self.upload_parquets_duration)

    def get_parquets_stats(self):
        return (self.total_sent_parquets, self.total_sent_parquets_size,self.total_temp_parquets)

    def get_source_properties(self):
        return SourceProperties(self.src_type,self.src_name,self.src_schema,self.src_table,self.request,self.src_flag_incr,self.src_date_lastupdate)

    def get_bronze_properties(self):
        return BronzeProperties(self.env,self.bronze_schema,self.bronze_table,self.bucketname,self.bucket_file_path,self.parquet_file_name_template)

    def get_logger(self):
        return self.logger

    def get_bronze_lastupdated_row(self):
        last_date = None
        if not self.bronze_db_connection:
            return last_date
        last_date = self.bronze_db.get_table_max_column(self.bronze_table, self.src_date_criteria)
        return last_date

    def send_parquet_files_to_oci(self,verbose=None):
        self.upload_parquets_start = datetime.now()
        self.__update_sent_parquets_stats()
        # define your OCI config
        oci_config_path = self.bronze_config.get_oci_settings().config_path
        oci_config_profile = self.bronze_config.get_oci_settings().profile
        oci_compartment_id = self.bronze_config.get_oci_settings().compartment_id

        if not self.parquet_file_list:
            vError = "WARNING, No parquet files to upload"
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message="")
            self.logger.log(error=Exception(vError), action=vError)
            return False
        self.parquet_file_list_tosend = []
        if not self.src_flag_incr:
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
            bucket = OCIBucket(self.bucketname, forcecreate=True,compartment_id=oci_compartment_id,file_location=oci_config_path, oci_profile=oci_config_profile)
            for p in self.parquet_file_list_tosend:
                # Sending parquet files
                    bucket_file_name = self.bucket_file_path +  p["file_name"]
                    source_file = p["source_file"]

                    message = "Uploading parquet from {0} into bucket {1}, {2}".format(source_file,self.bucketname,bucket_file_name)
                    if verbose:
                        verbose.log(datetime.now(tz=timezone.utc),"UPLOAD_PARQUET","START",log_message=message)
                    bucket.put_file(bucket_file_name, source_file)
        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR Uplaoding parquet file {0} into bucket {1}, {2}".format(source_file,self.bucketname,bucket_file_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message=str(err))
            self.logger.log(error=err, action=vError)
            return False
        self.__update_sent_parquets_stats()
        return True

    def update_bronze_schema(self,verbose):
        try:
            message = "Update Bronze schema {} : Incremental {}".format(self.bronze_table, bool(self.src_flag_incr))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "START", log_message=message)
            if not self.parquet_file_list:
                message = "No parquet files No update of Bronze needed"
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "END", log_message=message)
                res = True
            elif self.src_flag_incr and self.__is_bronzetable_exists__(self.bronze_table):
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
            self.logger.log(error=err, action=vError)
            return False

    def pre_fetch_source(self):
        # TO BE EXECUTE subclass fetch method
        # Request data from source
        # and generate local parquet files
        self.fetch_start = datetime.now()
        #delete old temporary parquet files
        self.__clean_temporary_parquet_files__()

    def fetch_source(self,verbose=None):
        pass

class BronzeGenerator:
    def __init__(self, vBronzeSourceBuilder, vBronzeExploit,vLogger=None):
        self.__bronzesourcebuilder__ = vBronzeSourceBuilder
        self.__bronzeexploit__ = vBronzeExploit
        self.__logger__ = vLogger

    def generate(self,verbose=None):
        while True:
            generate_result = False
            # 1 Fetch data from source
            self.__bronzesourcebuilder__.pre_fetch_source()
            if not self.__bronzesourcebuilder__.fetch_soure(verbose):
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

                if not self.__bronzeexploit__.update_exploit(vSourceProperties.name, vSourceProperties.schema, vSourceProperties.table,
                                              "SRC_DATE_LASTUPDATE", last_date):
                    break
            generate_result = True
            break
        self.__bronzesourcebuilder__.update_total_duration()
        if generate_result:
            if verbose:
                print(vSourceProperties)
                message = "Integrating {3} rows from {0} {1} {2} in {4}".format(vSourceProperties.name, vSourceProperties.schema,
                                                                         vSourceProperties.table, self.__bronzesourcebuilder__.get_rows_stats()[0],self.__bronzesourcebuilder__.get_durations_stats()[0])
                verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "END", log_message=message)
            if self.__logger__:
                self.__logger__.log()
        return generate_result

