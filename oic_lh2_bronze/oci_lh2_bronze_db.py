import pandas as pd
from nlspackage.nlsdatabronze.oci_lh2_bronze import *

class BronzeSourceBuilderDb(BronzeSourceBuilder):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode,logger):
        super().__init__(br_config, "DB", src_name, src_origin_name, src_table_name, src_table_where,
                         src_flag_incr, src_date_where, src_date_lastupdate, force_encode,logger)
        self.bronze_table = self.src_name + "_" + self.src_schema + "_" + self.src_table.replace(" ", "_")
        self.bucket_file_path = self.src_schema + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()

        # Create connexion to source database
        self.source_database_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(),self.src_name)
        self.source_db = DBFACTORY.create_instance(self.source_database_param.dbwrapper,self.bronze_config.get_configuration_file())
        self.source_db_connection = self.source_db.create_db_connection(self.source_database_param)

        if not self.source_db_connection:
            message = "Error connecting to : {}".format(self.src_name)
            self.logger.log_error(error=message, action="Error DB connection")
            raise(message)
        # Customize select to force encode of columns
        self.__custom_select_from_source__()

    def fetch_source(self,verbose=None):
        try:
            if not self.source_db_connection:
                raise
            # Execute a SQL query to fetch all data from the current table
            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.src_schema, self.src_table,SQL_READMODE)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,log_request = self.request)
            self.df_table_content = pd.DataFrame()
            match SQL_READMODE:
                case "DBCURSOR":
                    cur = self.source_db_connection.cursor()
                    cur.arraysize = DB_ARRAYSIZE
                    if self.db_execute_bind:
                        # where clause on date
                        cur.execute(self.request,self.db_execute_bind)
                    else:
                        cur.execute(self.request)
                    while True:
                        # Fetch data by chunk and convert to dataframe
                        # chunk specified by DB_ARREYSIZE
                        rows = cur.fetchmany()
                        #map column name from cursor to df ; force types of columns based on cursor type
                        self.df_table_content = self.source_db.create_df_with_db_cursor(rows,cur.description)
                        if self.df_table_content.empty:
                            break
                        # update total count of imported rows
                        self.__update_fetch_stats__()
                        elapsed = datetime.now() -self.fetch_start
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_rows_imported,elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
                        # create parquet file for current chunk dataframe
                        res = self.__create_parquet_file__(verbose)
                        if not res:
                            raise
                    cur.close()
                case "PANDAS":
                   # Fetch data by chunk and convert to dataframe
                    for df_chunk in pd.read_sql(sql=self.request,con=self.source_db_connection,chunksize=PANDAS_CHUNKSIZE):
                        self.df_table_content = pd.concat([self.df_table_content,df_chunk])
                        ## update total count of imported rows
                        self.__update_fetch_stats__()
                        elapsed = datetime.now() - self.fetch_start
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_rows_imported,elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
                        # create parquet file for current chunk dataframe
                        self.__create_parquet_file__(verbose)
            return True
        except UnicodeDecodeError as err:  # Catching Unicode Decode Error
            message = "Unicode Decode Error: {} ".format(str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "ERROR", log_message=message)
            self.logger.log_error(error=message, action="fetch_source Unicode Error")
            self.__update_fetch_stats__()
            return False
        except oracledb.Error as err:
            message = "Error fetch, Oracle DB error : {}".format(str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "ERROR", log_message=message,log_request = self.request)
            self.logger.log_error(error=message, action = "Oracle error")
            self.__update_fetch_stats__()
            return False
        except Exception as err:
            message = "Error Fetch: {}".format(str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "ERROR", log_message=message)
            self.logger.log_error(error=message, action = "Error fetch")
            self.__update_fetch_stats__()
            return False