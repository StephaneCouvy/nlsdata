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
    def __init__(self, pBronze_Config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager,pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where, pSrc_flag_incr,
                 pSrc_date_where, pSrc_date_lastupdate, pForce_encode, pLogger):
        super().__init__(pBronze_Config, pBronzeDb_Manager, pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where, pSrc_flag_incr,
                         pSrc_date_where, pSrc_date_lastupdate, pForce_encode, pLogger)

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
