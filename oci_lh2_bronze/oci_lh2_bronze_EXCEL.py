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
    def __init__(self, pSourceProperties:SourceProperties, pBronze_config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager,pLogger:BronzeLogger):
        super().__init__(pSourceProperties,pBronze_config, pBronzeDb_Manager,pLogger)

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
            _df = pd.read_excel(_file, sheet_name=_wrksheet, **file_read_options)
            return _df
        else:
            raise ValueError("No file path provided")
