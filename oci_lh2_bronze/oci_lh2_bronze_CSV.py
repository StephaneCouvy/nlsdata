import pandas as pd

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
from nlsdata.oci_lh2_bronze.oci_lh2_bronze_file import *

class BronzeSourceBuilderFileCSV(BronzeSourceBuilderFile):
    '''BronzeSourceBuilderFileCSV class'''

    def __init__(self, pSourceProperties:SourceProperties, pBronze_config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager, pLogger:BronzeLogger):
        '''BronzeSourceBuilderFileCSV constructor'''
        super().__init__(pSourceProperties, pBronze_config, pBronzeDb_Manager, pLogger)


    def __import_file__(self, *fileargs, **file_read_options):
        '''This method reads CSV files from the specified directory fileargs[0].'''

        if fileargs:
            # Determine file type based on source name
            _file = fileargs[0]
            _df = pd.read_csv(_file, **file_read_options)

            return _df

        else:
            raise ValueError("No file path provided")




