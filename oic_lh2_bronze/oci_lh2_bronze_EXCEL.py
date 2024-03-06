import pandas as pd
import oci
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze_file import BronzeSourceBuilderFile
from NLSOCI.oci_bucket import OCIBucket, OCIEnv
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from NLSOCI import *
from datetime import datetime, timezone
import io
from NLSOCI.utils import (recover_files_from_bucket,decompress_all_files)
import os
from NLSOCI.oci_bucket import OCI_ReportUsage_Bucket
from NLSTOOLS.tool_kits import *
import openpyxl


class BronzeSourceBuilderFileEXCEL(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)


    def __import_file__(self,*fileargs):
        """
         Cette méthode lit les fichiers excel et turkey
         """
        match self.src_name:
            case "TURKEY":
                table = pd.read_excel(self.src_schema, sheet_name=self.src_table, skiprows=1)  # adapt with excel table
            case "EXCEL":
                wb = openpyxl.load_workbook(self.src_schema)
                print(wb.sheetnames)
                for sheet_name in wb.sheetnames:
                    table = pd.read_excel(self.src_schema, sheet_name=sheet_name, skiprows=0)
                    return table
            case _:
                return False


#Création de l'environnement de test pour tester les fonctions au dessus
configuration_file = 'C:\Python\projects\LH2Bronze_Loader\config_PCBenjamin.json'
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
src_type = "FILE_EXCEL"
src_name = "EXCEL"
src_origin_name = 'C:\Python\projects\LH2Bronze_Loader\\files\GROUP_NAME.xlsx'
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
                                     src_flag_incr, src_date_criteria,src_date_lastupdate, force_encode, logger)
#print(bronze_builder)

#Création du BronzeSourceBuilderFile à partir du BronzeSourceBuilder
bronze_source_builder_file = BronzeSourceBuilderFile(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema, bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                 src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

#Création du BronzeSourceBuilderFileCSV
source = BronzeSourceBuilderFileEXCEL(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema,
                                      bronze_builder.src_table, bronze_builder.src_table_whereclause,
                                      bronze_builder.src_flag_incr, src_date_where,
                                      bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

# Appel de la méthode fetch_source de BronzeSourceBuilderFileEXCEL
source.fetch_source(verbose)
