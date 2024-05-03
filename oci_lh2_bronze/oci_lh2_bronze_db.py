import pandas as pd
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *

class BronzeSourceBuilderDb(BronzeSourceBuilder):
    def __init__(self, pBronze_Config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager, pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where, pSrc_flag_incr,
                 pSrc_date_where, pSrc_date_lastupdate, pForce_encode,pLogger):
        super().__init__(pBronze_Config, pBronzeDb_Manager, "DB", pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where,
                         pSrc_flag_incr, pSrc_date_where, pSrc_date_lastupdate, pForce_encode,pLogger)

        # Create connexion to source database
        self.source_database_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),self.src_name)
        self.source_db = DBFACTORY.create_instance(self.source_database_param.dbwrapper,self.bronze_config.get_configuration_file())
        self.source_db_connection = self.source_db.create_db_connection(self.source_database_param)

        if not self.source_db_connection:
            vError = "ERROR connecting to : {}".format(self.src_name)
            raise Exception(vError)
        # Customize select to force encode of columns
        self.__custom_select_from_source__()

    def __set_bronze_table_settings__(self):
        #define bronze table name, bucket path to add parquet files, get index to restart parquet files interation
        self.bronze_table = self.src_name + "_" + self.src_schema + "_" + self.src_table.replace(" ", "_")
        # define template name of parquet files
        self.parquet_file_name_template = self.src_name + "_" + self.src_table.replace(" ", "_")
        self.parquet_file_id = 0
        # Define the path for storing parquets files in the bucket
        self.bucket_file_path = self.src_schema + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        # Get the index of the last Parquet file in the bucket
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()
        
    def fetch_source(self,verbose=None):
        try:
            if not self.source_db_connection:
                raise Exception("Error no DB connection")
            # Execute a SQL query to fetch all data from the current table
            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.src_schema, self.src_table,SQL_READMODE)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,log_request = self.request)
            self.df_table_content = pd.DataFrame()
            match SQL_READMODE:
                case "DBCURSOR":
                    cursor = self.source_db_connection.cursor()
                    cursor.arraysize = DB_ARRAYSIZE
                    if self.db_execute_bind:
                        # where clause on date
                        cursor.execute(self.request,self.db_execute_bind)
                    else:
                        cursor.execute(self.request)
                    while True:
                        # Fetch data by chunk and convert to dataframe
                        # chunk specified by DB_ARREYSIZE
                        rows = cursor.fetchmany()
                        #map column name from cursor to df ; force types of columns based on cursor type
                        self.df_table_content = self.source_db.create_df_with_db_cursor(rows,cursor.description)
                        if self.df_table_content.empty:
                            break
                        # create parquet file for current chunk dataframe
                        res = self.__create_parquet_file__(verbose)
                        if not res:
                            raise Exception("Error creating parquet file")
                        # update total count of imported rows
                        self.__update_fetch_row_stats__()
                        elapsed = datetime.now() -self.fetch_start
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
                    cursor.close()
                case "PANDAS":
                   # Fetch data by chunk and convert to dataframe
                    for df_chunk in pd.read_sql(sql=self.request,con=self.source_db_connection,chunksize=PANDAS_CHUNKSIZE):
                        self.df_table_content = pd.concat([self.df_table_content,df_chunk])
                        # create parquet file for current chunk dataframe
                        res = self.__create_parquet_file__(verbose)
                        if not res:
                            raise Exception("Error creating parquet file")
                        ## update total count of imported rows
                        self.__update_fetch_row_stats__()
                        elapsed = datetime.now() - self.fetch_start
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
            return True
        except UnicodeDecodeError as err:  # Catching Unicode Decode Error
            vError = "ERROR Unicode Decode, table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False
        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message='Oracle DB error :{}'.format(str(err)),log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False
        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False