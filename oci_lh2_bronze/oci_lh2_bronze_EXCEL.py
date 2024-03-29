import pandas as pd
import oci
from nlsdata.oic_lh2_bronze.oci_lh2_bronze_file import BronzeSourceBuilderFile
from nlsoci.oci_bucket import OCIBucket, OCIEnv
from nlsdata.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from nlsoci import *
from datetime import datetime, timezone
import io
from nlsoci.utils import (recover_files_from_bucket, decompress_all_files)
import os
from nlsoci.oci_bucket import OCI_ReportUsage_Bucket
from nlstools.tool_kits import *
import openpyxl


# Define a class BronzeSourceBuilderFileEXCEL inheriting from BronzeSourceBuilderFile
class BronzeSourceBuilderFileEXCEL(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)

    # Method to import Excel or Turkey files
    def __import_file__(self, *fileargs):
        """
        This method reads Excel and Turkey files.
        """
        # Determine file type based on source name
        _file = fileargs[0]
        # Defining the sheet_name as the second argument (src_table)
        _wrksheet = fileargs[1]
        match self.src_name:
            case "TURKEY":
                # Read Excel file with skipping the first row
                table = pd.read_excel(_file, sheet_name=_wrksheet, skiprows=1)
            case "EXCEL":
                # Read Excel file without skipping rows
                table = pd.read_excel(_file, sheet_name=_wrksheet, skiprows=0)
            case _:
                return None
        return table





