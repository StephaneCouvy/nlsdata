import pandas as pd
import oci
from oci_lh2_bronze_file import BronzeSourceBuilderFile
from nlsoci.oci_bucket import OCIBucket, OCIEnv
from nlsdata.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from nlsoci import *
from datetime import datetime, timezone

class BronzeSourceBuilderFileFromBucket(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)

    def bucket_connection(self, bucket_name):
        """
        Connection to a OCI Bucket.
        """
        oci_config_path = "config_oci.txt"
        oci_config_profile = "DEBUG"
        try:
            bucket = OCIBucket(bucket_name, file_location=oci_config_path, oci_profile=oci_config_profile)
            return bucket

        except Exception as e:
            print(f"Error connecting to bucket {bucket_name}: {str(e)}")
            return None

    def fetch_source(self, bucket_name, verbose=None):
        """
        Revover csv file from bucket and extract the data from the file into a dataframe, then create a parquet file with this data.
        """
        try:

            bucket = self.bucket_connection(bucket_name) #Recover bucket
            if bucket.buckets_exists():

                if verbose:
                    message = "Extracting data from file {0},{1},{2}".format(self.src_name, self.src_schema, self.src_table)
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)

                if self.src_name == "CSV":
                    table = pd.read_csv() #Saisir comme paramètre de la focntion un chemin qui pointe vers le fichier csv
                else:
                    vError = "ERROR Unknown source {0}, extracting data from file {1},{2}".format(self.src_name,
                                                                                              self.src_schema,
                                                                                              self.src_table)
                    if verbose:
                        verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message='')
                    self.logger.log(error=Exception('Unkown source'), action=vError)
                    return False

                self.df_table_content = table.astype('string')
                res = self.__create_parquet_file__()
                if not res:
                    raise Exception("Failed to create Parquet file")

                self.__update_fetch_row_stats__()
                return True
        except Exception as err:
            vError = "ERROR Extracting from {0}, file {1},{2} : {3}".format(self.src_name, self.src_schema,
                                                                            self.src_table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err))
            self.logger.log(error=err, action=vError)
            self.__update_fetch_row_stats__()
            return False

