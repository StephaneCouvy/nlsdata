import pandas as pd
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import *
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze_CSV import BronzeSourceBuilderFileCSV
from NLSOCI.utils import recover_files_from_bucket

# Define a class BronzeSourceBuilderBucketCSV inheriting from BronzeSourceBuilderFileCSV
class BronzeSourceBuilderBucketCSV(BronzeSourceBuilderFileCSV):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, src_name, src_origin_name, src_table_name, src_table_where,
                         src_flag_incr, src_date_where, src_date_lastupdate, force_encode, logger)

        # Initialize the bucket name
        self.bucketname = self.bronze_config.get_oci_settings().bucket_debug
        self.oci_compartment_id = self.bronze_config.get_oci_settings().compartment_id
        self.oci_config_path = self.bronze_config.get_oci_settings().config_path
        self.oci_config_profile = self.bronze_config.get_oci_settings().profile
        self.bucket = OCIBucket(self.bucketname, forcecreate=True,compartment_id=self.oci_compartment_id,file_location=self.oci_config_path, oci_profile=self.oci_config_profile)

    def recover_files(self):
        """
        This method recovers files from the specified bucket and saves them to the local directory.
        """
        # Call utility function to recover files from the bucket and save them locally
        recover_files_from_bucket(self.bucket, 'C:\Python\projects\LH2Bronze_Loader\\files')
