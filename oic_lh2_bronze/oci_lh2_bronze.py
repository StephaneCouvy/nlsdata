import os.path

import pandas as pd

from nlsoci.oci_bucket import *
from nlstools.config_settings import *
from nlstools.tool_kits import *
from nlsdb.dbwrapper_factory import *

PARQUET_IDX_DIGITS = 5
PANDAS_CHUNKSIZE = 100000
# Variables could be redefined by json config file
DB_ARRAYSIZE = 50000
SQL_READMODE = "DBCURSOR" #CURSORDB / PANDAS

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

class BronzeExploit:
    # Iterator object for list of sources to be imported into Bronze
    # Define metohd to update src_dat_lastupdate for table with incremental integration

    def __init__(self,br_config,verbose=None):
        self.idx = 0
        self.bronze_config = br_config
        self.exploit_db_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),"exploit")
        self.db = DBFACTORY.create_instance(self.exploit_db_param.dbwrapper,self.bronze_config.get_configuration_file())
        self.oracledb_connection = self.db.create_db_connection(self.exploit_db_param)

        self.table_name = self.bronze_config.get_options().datasource_load_tablename_prefix + self.bronze_config.get_options().environment
        cur = self.oracledb_connection.cursor()

        # Execute a SQL query to fetch activ data from the table "LIST_DATASOURCE_LOADING_..." into a dataframe
        param_req = "select * from " + self.table_name + " where SRC_FLAG_ACTIV = 1 ORDER BY SRC_TYPE,SRC_NAME,SRC_OBJECT_NAME"
        cur.execute(param_req)
        self.df_param = pd.DataFrame(cur.fetchall())
        self.df_param.columns = [x[0] for x in cur.description]
        cur.close()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            items = [self.df_param.iloc[self.idx,i] for i in range(len(self.df_param.columns))]
        except IndexError:
            raise StopIteration()
        self.idx += 1
        return items

    def update_exploit(self,src_name,src_origin_name,src_object_name,column_name, value,verbose=None):
        res = False
        request = ""
        try:
            cur = self.oracledb_connection.cursor()
            request = "UPDATE " + self.table_name + " SET "+column_name+" = :1 WHERE SRC_NAME = :2 AND SRC_ORIGIN_NAME = :3 AND SRC_OBJECT_NAME = :4"

            # update last date or creation date (depends on table)
            message = "Updating {} = {} on table {}".format(column_name,value,self.table_name)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SET_LASTUPDATE", "START", log_message=message,
                            log_request=request)
            cur.execute(request,(value,src_name,src_origin_name,src_object_name))
            self.oracledb_connection.commit()
            res = True
            cur.close()
            return res

        except oracledb.Error as err:
            message = "Error set last updated row for table {} :{}".format(self.table_name, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "GET_MAX_COLUMN", "ERROR", log_message=message,
                            log_request=request)
            raise
        except Exception as err:
            message = "Error set last updated row for table {} :{}".format(self.table_name, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SET_MAX_COLUMN", "ERROR", log_message=message,
                            log_request=request)
            return res

class BronzeLogger():
    def __init__(self, bronze_config,verbose=None):
        self.bronze_source = None
        self.bronze_config = bronze_config
        self.verbose = verbose
        self.cur = None
        self.oracledb_connection = None
        if self.bronze_config:
            self.env = bronze_config.get_options().environment
            self.table_name = self.bronze_config.get_options().log_table_prefix + self.bronze_config.get_options().environment
        self._init_logger()

    def __del__(self):
        if self.cur:
            self.cur.close()
    def _init_logger(self):
        if self.bronze_config:
            self.__del__()
            # Establish a connection to EXPLOIT schema to log message
            self.logger_db_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),
                                                                           self.bronze_config.get_options().logger_db)
            self.db = DBFACTORY.create_instance(self.logger_db_param.dbwrapper,self.bronze_config.get_configuration_file())

            self.oracledb_connection = self.db.create_db_connection(self.logger_db_param)
            self.cur = self.oracledb_connection.cursor()

            #Generate attributs of logger from table
            attr_list = list(self.db.get_table_columns(self.table_name).keys())
            self.BronzeLoggerProperties = namedtuple('BronzeLoggerProperties',attr_list)
            self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),END_TIME=None, ENVIRONMENT=self.env,ACTION='',SRC_NAME='',SRC_ORIGIN_NAME='',SRC_OBJECT_NAME='',REQUEST='',ERROR_TYPE='',ERROR_MESSAGE='',STAT_ROWS_COUNT=0,STAT_ROWS_SIZE=0,STAT_TOTAL_DURATION=0,STAT_FETCH_DURATION=0,STAT_UPLOAD_PARQUETS_DURATION=0,STAT_SENT_PARQUETS_COUNT=0,STAT_SENT_PARQUETS_SIZE=0)

    def link_to_bronze_source(self,br_source):
        self.bronze_source = br_source
        self.bronze_config = br_source.get_bronze_config()
        self.env = self.bronze_source.get_bronze_properties().environment
        vSourceProperties = self.bronze_source.get_source_properties()
        self._init_logger()
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=vSourceProperties.name,SRC_ORIGIN_NAME=vSourceProperties.schema,SRC_OBJECT_NAME=vSourceProperties.table)


    def __log__(self):
        vSourceDurationsStats = self.bronze_source.get_durations_stats()
        vSourceRowsStats = self.bronze_source.get_rows_stats()
        vSourceParquetsStats = self.bronze_source.get_parquets_stats()
        vSourceProperties = self.bronze_source.get_source_properties()

        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(REQUEST=vSourceProperties.request,ACTION=self.action,END_TIME=datetime.now(tz=timezone.utc),STAT_ROWS_COUNT=vSourceRowsStats[0],STAT_ROWS_SIZE=vSourceRowsStats[1],STAT_SENT_PARQUETS_COUNT=vSourceParquetsStats[0],STAT_SENT_PARQUETS_SIZE=vSourceParquetsStats[1],STAT_TOTAL_DURATION=vSourceDurationsStats[0],STAT_FETCH_DURATION=vSourceDurationsStats[1],STAT_UPLOAD_PARQUETS_DURATION=vSourceDurationsStats[2])
        self.__insertlog__()

    def log(self,action="SUCCESS",error=None):
        if error:
            self.error_type = type(error).__name__
            self.error_message = str(error)
        self.action = action
        self.__log__()

    def __insertlog__(self):
        # Define the SQL query to insert a log into the table
        columns = ''
        for col in self.instance_bronzeloggerproperties._fields:
            columns += col + ', '
        columns = columns.rstrip(', ')

        args = ''
        for i in range(1,len(self.instance_bronzeloggerproperties._fields)+1):
            args += ':{}, '.format(i)
        args  = args.rstrip(', ')

        sql = f"""INSERT INTO {self.table_name} ({columns}) VALUES ({args})"""

        #print(sql)
        #print(tuple(self.instance_bronzeloggerproperties))
        #print(self.instance_bronzeloggerproperties._asdict())
        # Execute the SQL query with the provided parameters
        self.cur.execute(sql, tuple(self.instance_bronzeloggerproperties))

        # Commit the transaction to the database
        self.oracledb_connection.commit()

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

        self.total_parquet_sent = 0 # Total number of parquet files sent to bucket
        self.total_parquet_sent_size = 0 # total size of parquet files
        self.total_rows_imported = 0  # Total number of rows imported from the source table
        self.total_rows_size_imported = 0 # size of importted rows from source
        self.start = datetime.now()
        self.fetch_start = None
        self.send_parquet_start = None
        self.total_duration = datetime.now() - datetime.now() # duration of requests
        self.fetch_duration = datetime.now() - datetime.now() # duration to ftech data and create local parquet files
        self.send_parquet_duration = datetime.now() - datetime.now() # duration to send parquet files to OCI
        # if debug = True then we use a dedicated bucket to store parquet files. Bucket name identified into json config
        self.debug = self.bronze_config.isdebugmode()
        # Define bucket
        if not self.debug:
            self.bucketname = "Bucket-" + self.env + "-" + self.src_name
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

        # setup logger to log events errors and success into LOG_PYTHON_env table
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
        self.total_rows_imported += len(self.df_table_content)
        self.total_rows_size_imported += int(self.df_table_content.memory_usage(deep=True).sum())
        if self.fetch_start:
            self.fetch_duration = datetime.now() - self.fetch_start

    def __update_sent_parquets_stats(self):
        self.total_parquet_sent = len(self.parquet_file_list_tosend)
        for p in self.parquet_file_list_tosend:
            source_file = p["source_file"]
            self.total_parquet_sent_size += os.path.getsize(source_file)
            # Deleting temp file
            os.remove(source_file)
        if self.send_parquet_start:
            self.send_parquet_duration = datetime.now() - self.send_parquet_start

    def __add_integration_date_column__(self):
        # Adding column into every parquet file with System date to log integration datetime
        integration_time = datetime.now(tz=timezone.utc)
        self.df_table_content["fetch_date"] = integration_time

    def __get_last_parquet_idx_in_bucket__(self):
        # check if other parquet files already exist into the same bucket folder
        # get the last id parquet number
        # to avoid remplace existing parquet files
        idx = 0
        # define your OCI config
        oci_config_path = self.bronze_config.get_oci_settings().config_path
        oci_config_profile = self.bronze_config.get_oci_settings().profile
        bucket = OCIBucket(self.bucketname, file_location=oci_config_path, oci_profile=oci_config_profile)

        what_to_search = self.bucket_file_path+self.parquet_file_name_template
        list_buckets_files = [obj.name for obj in bucket.list_objects(what_to_search)]
        if list_buckets_files:
            try:
                max_file = max(list_buckets_files)
                pos = re.search(what_to_search, max_file).span()[1]
                idx = int(max_file[pos:pos + PARQUET_IDX_DIGITS])
            except ValueError:
                idx = 0
        return idx

    def __clean_temporary_parquet_files__(self):
        # clean temporay local parquet files (some could remain from previous failure process
        merged_parquet_file_name = self.parquet_file_name_template + ".parquet"
        merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
        list_files = list_template_files(os.path.splitext(merged_parquet_file)[0])
        for f in list_files:
            os.remove(f)

    def __create_parquet_file__(self,verbose=None):
        # Parquet file si created locally into local_workingdir
        # Define the path and filename for the Parquet file, id on 4 digitis
        self.parquet_file_id += 1

        parquet_file_name= self.parquet_file_name_template + '{:04d}'.format(self.parquet_file_id) + ".parquet"
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
                message = "Now row for {}".format(parquet_file_name)
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", "START", log_message=message)
                return False
        except OSError as err:
            message = "ERROR creating parquet file : {}".format(str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", "ERROR",log_message=message)
            self.logger.log(error=err, action = message)
            return False
        except Exception as err:
            message = "ERROR creating parquet file {0}: {1}".format(parquet_file_name,str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", "ERROR",log_message=message)
            self.logger.log(error=err, action = message)
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
            cur = self.bronze_db_connection.cursor()

            request = 'BEGIN DBMS_CLOUD.SYNC_EXTERNAL_PART_TABLE(table_name =>\'' + table + '\'); END;'
            cur.execute(request)

            # Execute a PL/SQL to synchronize Oracle partionned external table
            if verbose:
                message = "Synchronizing external partionned table {}".format(table)
                verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE", "SYNC", log_message=message)
            cur.execute(request)
            cur.close()
            return True

        except oracledb.Error as err:
            message = "ERROR synchronizing table {}, Oracle DB error : {}".format(table,str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE", "ERROR", log_message=message)
            self.logger.log(error=err, action=message)
            raise
        except Exception as err:
            message = "ERROR synchronizing table {} : {}".format(table,str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE", "ERROR", log_message=message)
            self.logger.log(error=err, action=message)
            return False

    def __create_bronze_table__(self,verbose=None):
        table = self.bronze_table
        try:
            if not self.bronze_db_connection:
                raise
            cur = self.bronze_db_connection.cursor()
            # Dropping table before recreating
            drop = 'BEGIN EXECUTE IMMEDIATE \'DROP TABLE ' + table + '\'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;'
            if verbose:
                message = "Dropping table {} : {}".format(table,drop)
                verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=message)
            cur.execute(drop)

            # Create external part table parsing parquet files from bucket root (for incremental mode)
            if self.src_flag_incr:
                # Create external part table parsing parquet files from bucket root (for incremental mode)
                root_path = self.bucket_file_path.split("/")[0]+"/"
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(table_name =>\'' + table + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/'+root_path+'*' + self.parquet_file_name_template + '*.parquet\', format => \'{"type":"parquet", "schema": "first","partition_columns":[{"name":"fetch_year","type":"varchar2(100)"},{"name":"fetch_month","type":"varchar2(100)"},{"name":"fetch_day","type":"varchar2(100)"}]}\'); END;'
                # create += 'EXECUTE IMMEDIATE '+ '\'CREATE INDEX fetch_date ON ' + table + '(fetch_year,fetch_month,fetch_date)\'; END;'
                # not supported for external table
            else:
                # Create external table linked to ONE parquet file (for non incremental mode)
                root_path = self.bucket_file_path
                #create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + table + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/'+root_path+ self.parquet_file_name_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + table + '\',credential_name =>\'' + self.env + '_CRED_NAME\', file_uri_list =>\'https://frysfb5gvbrr.objectstorage.eu-frankfurt-1.oci.customer-oci.com/n/frysfb5gvbrr/b/' + self.bucketname + '/o/' + root_path + self.parquet_file_name_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
            if verbose:
                message = "Creating table {} : {}".format(table,create)
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "START", log_message=message)
            cur.execute(create)
            # Alter column type from BINARY_DOUBLE to NUMBER
            alter_table = 'BEGIN ADMIN.ALTER_TABLE_COLUMN_TYPE(\'' + self.bronze_database_param.p_username + '\',\'' + table + '\',\'BINARY_DOUBLE\',\'NUMBER\'); END;'
            if verbose:
                message = "Altering table columns type {}.{} : {}".format(self.bronze_database_param.p_username,table,alter_table)
                verbose.log(datetime.now(tz=timezone.utc), "ALTER_TABLE", "START", log_message=message)
            cur.execute(alter_table)
            cur.close()
            return True

        except oracledb.Error as err:
            message = "ERROR creating table {}, Oracle DB error : {}".format(table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "ERROR", log_message=message)
            self.logger.log(error=err, action=message)
            raise
        except Exception as err:
            message = "ERROR creating table {} : {}".format(table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "ERROR", log_message=message)
            self.logger.log(error=err, action=message)
            return False

    def update_total_duration(self):
        self.total_duration = datetime.now() - self.start

    def get_bronze_config(self):
        return self.bronze_config

    def get_rows_stats(self):
        return (self.total_rows_imported,self.total_rows_size_imported)

    def get_durations_stats(self):
        return (self.total_duration,self.fetch_duration,self.send_parquet_duration)

    def get_parquets_stats(self):
        return (self.total_parquet_sent,self.total_parquet_sent_size)

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
        self.send_parquet_start = datetime.now()
        self.__update_sent_parquets_stats()
        # define your OCI config
        oci_config_path = self.bronze_config.get_oci_settings().config_path
        oci_config_profile = self.bronze_config.get_oci_settings().profile

        if not self.parquet_file_list:
            message = "No parquet files to be sent"
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "SEND_PARQUET", "START", log_message=message)
                self.logger.log(action = "WARNING",error=Exception(message))
            return True
        self.parquet_file_list_tosend = []
        if not self.src_flag_incr:
            # if not incremental mode, merging parquet files into one, before sending
            merged_parquet_file_name = self.parquet_file_name_template+".parquet"
            merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
            # Use 1st alternative to merge parquet files
            #parquet_files_tomerge = [p["source_file"] for p in self.parquet_file_list]
            #merge_parquet_files(parquet_files_tomerge,merged_parquet_file,removesource=True,verbose)
            #########################
            # Use 2nd alternative to merge parquet file based on same root names - based on duckdb
            duckdb_db = DBFACTORY.create_instance(self.bronze_config.get_duckdb_settings().dbwrapper,self.bronze_config.get_configuration_file())
            duckdb_db_connection = duckdb_db.create_db_connection(self.bronze_config.get_duckdb_settings())
            merge_template_parquet_files(os.path.splitext(merged_parquet_file)[0],duckdb_db_connection,True,verbose)

            self.parquet_file_list_tosend = [{"source_file":merged_parquet_file,"file_name":merged_parquet_file_name}]
        else:
            self.parquet_file_list_tosend = self.parquet_file_list

        for p in self.parquet_file_list_tosend:
            # Sending parquet files
            try:
                bucket = OCIBucket(self.bucketname, file_location=oci_config_path, oci_profile=oci_config_profile)
                bucket_file_name = self.bucket_file_path +  p["file_name"]
                source_file = p["source_file"]

                message = "Sending parquet from {0} into bucket {1}, {2}".format(source_file,self.bucketname,bucket_file_name)
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc),"SEND_PARQUET","START",log_message=message)
                bucket.put_file(bucket_file_name, source_file)

            except Exception as err:
                message = "ERROR sending parquet file {0} into bucket {1}, {2} : {3}".format(source_file,self.bucketname,bucket_file_name,str(err))
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc),"SEND_PARQUET","ERROR",log_message=message)
                self.logger.log(error=err, action = message)
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
            message = "ERROR updating bronze schema for table : {}".format(self.bronze_table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "ERROR", log_message=message)
            self.logger.log(error=err, action=message)
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
        # 1 Extract data from source
        self.__bronzesourcebuilder__.pre_fetch_source()
        if not self.__bronzesourcebuilder__.fetch_source(verbose):
            raise Exception

        # 2 Push parquets files to bucket
        if not self.__bronzesourcebuilder__.send_parquet_files_to_oci(verbose):
            raise Exception

        # 3 - Create/Update external table into Autonomous
        if not self.__bronzesourcebuilder__.update_bronze_schema(verbose):
            raise Exception

        # 4 Update "Last_update" for incremental table integration
        vSourceProperties = self.__bronzesourcebuilder__.get_source_properties()
        if vSourceProperties.incremental:
            last_date = self.__bronzesourcebuilder__.get_bronze_lastupdated_row()
            print(last_date, type(last_date))
            self.__bronzeexploit__.update_exploit(vSourceProperties.name, vSourceProperties.schema, vSourceProperties.table,
                                          "SRC_DATE_LASTUPDATE", last_date, verbose)
        self.__bronzesourcebuilder__.update_total_duration()
        if verbose:
            print(vSourceProperties)
            message = "Integrating {3} rows from {0} {1} {2} in {4}".format(vSourceProperties.name, vSourceProperties.schema,
                                                                     vSourceProperties.table, self.__bronzesourcebuilder__.get_rows_stats()[0],self.__bronzesourcebuilder__.get_durations_stats()[0])
            verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "END", log_message=message)
        if self.__logger__:
            self.__logger__.log()

