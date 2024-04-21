from datetime import datetime, timezone
import io
import os
import pandas as pd
import oci

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
from nlsdata.oci_lh2_bronze.oci_lh2_bronze_file import *

class BronzeSourceBuilderFileCSV(BronzeSourceBuilderFile):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)


     # Method to import CSV files
    def __import_file__(self,*fileargs,**file_read_options):
        """
        This method reads CSV files from the specified directory fileargs[0].
        """
        if fileargs:
            # Determine file type based on source name
            _file = fileargs[0]
            _df = pd.read_csv(_file,**file_read_options)
            return _df
        else:
            raise ValueError("No file path provided")




