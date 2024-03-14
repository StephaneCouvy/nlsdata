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
        # Set the default directory for CSV files
        self.directory = 'files'

    # Method to import CSV files
    def __import_file__(self):
        """
        This method reads CSV files from the specified directory (src_schema).
        """
        dataframe = None

        try:
            if os.path.exists(self.directory):
                print("The path is valid.")

                # Loop through files in the directory
                for file_name in os.listdir(self.directory):
                    # Check if the file is a CSV file
                    if file_name.endswith('.csv'):
                        # Read CSV file into a DataFrame
                        csv_dataframe = pd.read_csv(os.path.join(self.directory, file_name))

                        # If DataFrame is empty, assign the CSV DataFrame
                        if dataframe is None:
                            dataframe = csv_dataframe
                        # If DataFrame is not empty, concatenate CSV DataFrame with existing DataFrame
                        else:
                            dataframe = pd.concat([dataframe, csv_dataframe], ignore_index=True)
            else:
                print("The path is not valid or the directory does not exist")

        except Exception as e:
            print(f"An error occurred: {e}")

        return dataframe
