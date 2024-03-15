import pandas as pd
import oci
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze_file import BronzeSourceBuilderFile
from NLSOCI.oci_bucket import OCIBucket, OCIEnv
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from NLSOCI import *
from datetime import datetime, timezone
import io
import os
from NLSTOOLS.tool_kits import *


# Define a class BronzeSourceBuilderFileCSV inheriting from BronzeSourceBuilderFile
class BronzeSourceBuilderFileCSV(BronzeSourceBuilderFile):

    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                         src_date_where, src_date_lastupdate, force_encode, logger)

    # Method to import CSV files
    def __import_file__(self):
        """
        This method reads CSV files from the specified directory (src_schema).
        """
        try:
            if os.path.exists(self.src_schema):
                dataframe = pd.read_csv(self.src_schema)
                return dataframe
            else:
                raise FileNotFoundError("The path is not valid or the file does not exist")
        except Exception as e:
            print("An error occurred:", str(e))
            return None
