import pandas as pd
from nlsdata.oic_lh2_bronze.oci_lh2_bronze_file import BronzeSourceBuilderFile
from nlsdata.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from nlstools.tool_kits import *


# Define a class BronzeSourceBuilderFileCSV inheriting from BronzeSourceBuilderFile
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
        try:
            if fileargs:
                # Determine file type based on source name
                _file = fileargs[0]
                if os.path.exists(_file):
                    dataframe = pd.read_csv(_file)
                    return dataframe
                else:
                    raise FileNotFoundError("The path is not valid or the file does not exist")
            else:
                raise ValueError("No file path provided")
        except Exception as e:
            print("An error occurred:", str(e))
            return None

