import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *


class BronzeSourceBuilderRestAPI(BronzeSourceBuilder):
    def __init__(self, pSourceProperties: SourceProperties, pBronze_config: BronzeConfig,
                 pBronzeDb_Manager: BronzeDbManager, pLogger: BronzeLogger):
        vSourceProperties = pSourceProperties._replace(type="REST_API")
        super().__init__(vSourceProperties, pBronze_config, pBronzeDb_Manager, pLogger)
        self.source_database_param = get_parser_config_settings("rest_api")(self.bronze_config.get_configuration_file(),self.get_bronze_source_properties().name)
        self.url = self.source_database_param.url
        self.user = self.source_database_param.user
        self.password = self.source_database_param.password
        self.headers = self.source_database_param.headers
        self.endpoint = self.bronze_source_properties.table
        self.response = requests.get(self.url + self.endpoint, auth=HTTPBasicAuth(self.user, self.password), headers=self.headers)

        if self.response.status_code != 200:
            vError = "ERROR connecting to : {}".format(self.get_bronze_source_properties().name)
            raise Exception(vError)

    def __set_bronze_bucket_proxy__(self):
        # define settings for bucket, especially storagename... could depends on subclass
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.get_bronze_source_properties().name)

    def __set_bronze_table_settings__(self):
        # define bronze table name, bucket path to add parquet files, get index to restart parquet files interation
        v_bronze_table_name = self.get_bronze_source_properties().bronze_table_name
        if not v_bronze_table_name:
            v_bronze_table_name = self.bronze_table = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().schema + "_" + self.get_bronze_source_properties().table.replace(
                " ", "_")
        self.bronze_table = v_bronze_table_name.upper()
        # define template name of parquet files
        # self.parquet_file_name_template = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().table.replace(" ", "_")
        self.parquet_file_name_template = self.bronze_table
        self.parquet_file_id = 0
        # Define the path for storing parquets files in the bucket
        # self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        v_dict_externaltablepartition = self.get_externaltablepartition_properties()._asdict()
        self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + '/'.join(
            [f"{key}" for key in v_dict_externaltablepartition.values()]) + "/"
        # Get the index of the last Parquet file in the bucket
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()

    def fetch_source(self, verbose=None):
        try:
            if self.response.status_code != 200:
                raise Exception("Error no DB connection")
            # Execute a SQL query to fetch all data from the current table
            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.get_bronze_source_properties().schema,
                                                                      self.get_bronze_source_properties().table,
                                                                      SQL_READMODE)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,
                            log_request=self.request + ': ' + str(self.db_execute_bind))
            self.df_table_content = pd.DataFrame()
            data = self.response.json()
            items = data['items']
            self.df_table_content = pd.DataFrame(items)

            # create parquet file for current chunk dataframe
            res = self.__create_parquet_file__(verbose)
            if not res:
                raise Exception("Error creating parquet file")
            # update total count of imported rows
            self.__update_fetch_row_stats__()
            elapsed = datetime.now() - self.fetch_start
            if verbose:
                message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
            return True
        except UnicodeDecodeError as err:  # Catching Unicode Decode Error
            vError = "ERROR Unicode Decode, table {}".format(self.get_bronze_source_properties().table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False
        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,
                            log_message='Oracle DB error :{}'.format(str(err)), log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False
        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False