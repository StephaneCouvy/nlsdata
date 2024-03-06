import pandas as pd
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import *

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
            vError = "ERROR connecting to : {}".format(self.src_name)
            raise(vError)
        # Customize select to force encode of columns
        self.__custom_select_from_source__()

    def fetch_source(self, verbose=None):
        try:
            if not self.source_db_connection:
                raise Exception("Failed to connect to database")

            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.src_schema, self.src_table, SQL_READMODE)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,
                            log_request=self.request)

            self.df_table_content = pd.DataFrame()

            if SQL_READMODE == "DBCURSOR":
                cursor = self.source_db_connection.cursor()
                cursor.arraysize = DB_ARRAYSIZE
                if self.db_execute_bind:
                    cursor.execute(self.request, self.db_execute_bind)
                else:
                    cursor.execute(self.request)
                while True:
                    rows = cursor.fetchmany()
                    self.df_table_content = self.source_db.create_df_with_db_cursor(rows, cursor.description)
                    if self.df_table_content.empty:
                        break
                    res = self.__create_parquet_file__(verbose)
                    if not res:
                        raise Exception("Failed to create Parquet file")
                    self.__update_fetch_row_stats__()
                    elapsed = datetime.now() - self.fetch_start
                    if verbose:
                        message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                        verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
                cursor.close()
            elif SQL_READMODE == "PANDAS":
                for df_chunk in pd.read_sql(sql=self.request, con=self.source_db_connection,
                                            chunksize=PANDAS_CHUNKSIZE):
                    self.df_table_content = pd.concat([self.df_table_content, df_chunk])
                    res = self.__create_parquet_file__(verbose)
                    if not res:
                        raise Exception("Failed to create Parquet file")
                    self.__update_fetch_row_stats__()
                    elapsed = datetime.now() - self.fetch_start
                    if verbose:
                        message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                        verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)

            return True
        except UnicodeDecodeError as err:
            vError = "ERROR Unicode Decode, table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(error=err, action=vError)
            self.__update_fetch_row_stats__()
            return False
        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,
                            log_message='Oracle DB error :{}'.format(str(err)), log_request=self.request)
            self.logger.log(error=err, action=vError)
            self.__update_fetch_row_stats__()
            return False
        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.src_table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(error=err, action=vError)
            self.__update_fetch_row_stats__()
            return False
