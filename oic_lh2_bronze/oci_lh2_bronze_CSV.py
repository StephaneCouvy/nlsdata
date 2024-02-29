
import pandas as pd
import oci
from oci_lh2_bronze_file import BronzeSourceBuilderFile
from nlsoci.oci_bucket import OCIBucket, OCIEnv
from nlsdata.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from nlsoci import *
from datetime import datetime, timezone
import io
import os
from nlstools.tool_kits import *


class BronzeSourceBuilderFileCSV(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)



    def __import_file__(self):
        """
        Cette méthode lit les fichiers CSV
        """
        if os.path.exists(self.src_schema):
            print("Le chemin est valide.")
        else:
            print("Le chemin n'est pas valide ou le fichier n'existe pas")
        file = pd.read_csv(self.src_schema)
        print("Successfully read CSV file")
        return file



#Création de l'environnement de test pour tester les fonctions au dessus
configuration_file = 'C:\python\project\LH2Bronze_Loader\config_PCoumayma.json'
bronze_config = BronzeConfig(configuration_file)
#print(bronze_config)
bronze_logger = BronzeLogger(bronze_config)
#print(bronze_logger)

if bronze_config.get_options().verbose:
    verbose = Logger(bronze_config.get_options().verboselogfile)
else:
    verbose = None

#Instanciation des paramètres pour le BronzeourceBuilder
br_config = bronze_config
src_type = "FILE"
src_name = "CSV"
src_origin_name = 'C:\\python\\project\\nlspackages\\nlsdata\\oic_lh2_bronze\\decompressed files\\0001000000674801.csv'
src_table_name = "LH2_DATASOURCE_LOADING_DEBUG_OUMAYMA"
src_table_where = ""
src_flag_incr = False
src_date_criteria = ""
src_date_where = ""
src_date_lastupdate = ""
force_encode = False
logger = bronze_logger


#Création du BronzeSourceBuilder
bronze_builder = BronzeSourceBuilder(br_config, src_type, src_name, src_origin_name, src_table_name, src_table_where,
                                     src_flag_incr, src_date_criteria,src_date_lastupdate, force_encode, logger)
#print(bronze_builder)

#Création du BronzeSourceBuilderFile à partir du BronzeSourceBuilder
bronze_source_builder_file = BronzeSourceBuilderFile(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema, bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                 src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

#Création du BronzeSourceBuilderFileCSV
source=BronzeSourceBuilderFileCSV(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema,
                                                                    bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                                                                    src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode,

                                                                   logger)
source.fetch_source(verbose)




