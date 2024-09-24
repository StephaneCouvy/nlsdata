import pandas as pd

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
from nlsdata.oci_lh2_bronze.oci_lh2_bronze_file import *
from nlstools.tool_kits import *


# Define a class BronzeSourceBuilderFileEXCEL inheriting from BronzeSourceBuilderFile
class BronzeSourceBuilderFileEXCEL(BronzeSourceBuilderFile):
    '''BronzeSourceBuilderFileEXCEL class'''
    
    def __init__(self, pSourceProperties:SourceProperties, pBronze_config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager, pLogger:BronzeLogger):
        '''BronzeSourceBuilderFileEXCEL constructor'''
        super().__init__(pSourceProperties,pBronze_config, pBronzeDb_Manager, pLogger)
 

    def __import_file__(self, *fileargs, **file_read_options):
        '''This method reads Excel and Turkey files.'''

        if fileargs:
            # Determine file type based on source name
            _file = fileargs[0]

            # Defining the sheet_name as the second argument (src_table)
            _wrksheet = fileargs[1]
            file_read_options = dict_convert_values_str_to_int(file_read_options)

            # 'NA' value will be kept as is in the DataFrame, while other values ​​specified in na_values ​​will be converted to NaN.
            _df = pd.read_excel(_file, sheet_name=_wrksheet, keep_default_na=False, na_values=["", "N/A", "NULL", "NaN", None], **file_read_options)

            return _df
        else:
            raise ValueError("No file path provided") 
