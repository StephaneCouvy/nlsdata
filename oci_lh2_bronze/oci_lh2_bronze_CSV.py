
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


     # Method to import CSV files
    def __import_file__(self,*fileargs):
        """
        This method reads CSV files from the specified directory fileargs[0].
        """
        if fileargs:
            # Determine file type based on source name
            _file = fileargs[0]
            dataframe = pd.read_csv(_file)
            return dataframe
        else:
            raise ValueError("No file path provided")




