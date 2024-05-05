import pandas as pd
import oci
from oci_lh2_bronze_file import BronzeSourceBuilderFile
from NLSOCI.oci_bucket import OCIBucket, OCIEnv
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from NLSOCI import *
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
                    message = "Extracting data from file {0},{1},{2}".format(self.bronze_source_properties.name, self.bronze_source_properties.schema, self.bronze_source_properties.table)
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)

                if self.bronze_source_properties.name == "CSV":
                    table = pd.read_csv() #Saisir comme paramètre de la focntion un chemin qui pointe vers le fichier csv
                else:
                    vError = "ERROR Unknown source {0}, extracting data from file {1},{2}".format(self.bronze_source_properties.name,
                                                                                              self.bronze_source_properties.schema,
                                                                                              self.bronze_source_properties.table)
                    if verbose:
                        verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message='')
                    self.logger.log(pError=Exception('Unkown source'), pAction=vError)
                    return False

                self.df_table_content = table.astype('string')
                res = self.__create_parquet_file__()
                if not res:
                    raise Exception("Failed to create Parquet file")

                self.__update_fetch_row_stats__()
                return True
        except Exception as err:
            vError = "ERROR Extracting from {0}, file {1},{2} : {3}".format(self.bronze_source_properties.name, self.bronze_source_properties.schema,
                                                                            self.bronze_source_properties.table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False


#Création de l'environnement de test pour tester les fonctions au dessus
configuration_file = 'C:\Python\projects\LH2Bronze_Loader\config_PCBenjamin.json'
bronze_config = BronzeConfig(configuration_file)
bronze_logger = BronzeLogger(bronze_config)
print(bronze_logger.verbose)

#Instanciation des paramètres pour le BronzeourceBuilder
br_config = bronze_config
src_type = "FILE"
src_name = "CSV"
src_origin_name = "C:\\Users\\auffretb\Documents\\0001000000674520.csv"
src_table_name = "LH2_DATASOURCE_LOADING_DEBUG_BENJAMIN"
src_table_where = ""
src_flag_incr = False
src_date_criteria = ""
src_date_where = ""
src_date_lastupdate = ""
force_encode = False
logger = bronze_logger

#Création du BronzeSourceBuilder
bronze_builder = BronzeSourceBuilder(br_config, src_type, src_name, src_origin_name, src_table_name, src_table_where,
                                     src_flag_incr, src_date_criteria, src_date_lastupdate, force_encode, logger)

#Création du BronzeSourceBuilderFile à partir du BronzeSourceBuilder
bronze_source_builder_file = BronzeSourceBuilderFile(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema, bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                 src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

#Création du BronzeSourceBuilderFileFromBucket depuis le BronzeSourceBuilderFile
bronze_builder_file_from_bucket = BronzeSourceBuilderFileFromBucket(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema,
                                                                    bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                                                                    src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode,
                                                                    logger)

# Connexion au bucket
bronze_builder_file_from_bucket.bucket_connection('Bucket-Dev-Test-Benjamin-AUFFRET')

#Exécution de la méthode fetch_source pour créer un fichier parquet à partir d'un fichier csv
bronze_builder_file_from_bucket.fetch_source('Bucket-Dev-Test-Benjamin-AUFFRET', logger)

