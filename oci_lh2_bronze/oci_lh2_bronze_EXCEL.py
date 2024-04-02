import pandas as pd
import oci
import os
import io
import openpyxl
from datetime import datetime, timezone

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
from nlsdata.oci_lh2_bronze.oci_lh2_bronze_file import *


# Define a class BronzeSourceBuilderFileEXCEL inheriting from BronzeSourceBuilderFile
class BronzeSourceBuilderFileEXCEL(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)

    # Method to import Excel or Turkey files
    def __import_file__(self, *fileargs,**file_read_options):
        """
        This method reads Excel and Turkey files.
        """
        if fileargs:
            # Determine file type based on source name
            _file = fileargs[0]
            # Defining the sheet_name as the second argument (src_table)
            _wrksheet = fileargs[1]
            """"
            match self.src_name:
                case "TURKEY":
                    # Read Excel file with skipping the first row
                    #_df = pd.read_excel(_file, sheet_name=_wrksheet, skiprows=1)
                    _df = pd.read_excel(_file, sheet_name=_wrksheet, **file_read_options)
                case "EXCEL":
                    # Read Excel file without skipping rows
                    #_df = pd.read_excel(_file, sheet_name=_wrksheet, skiprows=0)
                    _df = pd.read_excel(_file, sheet_name=_wrksheet, skiprows=0)
                case _:
                    return None
            """
            _df = pd.read_excel(_file, sheet_name=_wrksheet, **file_read_options)
            return _df
        else:
            raise ValueError("No file path provided")
