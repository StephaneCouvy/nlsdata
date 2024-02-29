import pandas as pd
import oci
from oci_lh2_bronze_file import BronzeSourceBuilderFile
from nlsoci.oci_bucket import OCIBucket, OCIEnv
from nlsdata.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from nlsoci import *
from datetime import datetime, timezone
import io
from utils import (recover_files_from_bucket,decompress_all_files)
import os
from oci_bucket import OCI_ReportUsage_Bucket


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
                table = pd.read_excel(self.src_schema, sheet_name=self.src_table, skiprows=0)
            case _:
                return False
        return table





