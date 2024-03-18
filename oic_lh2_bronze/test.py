import pandas as pd
import oci
from oci_lh2_bronze_file import BronzeSourceBuilderFile
from oci_lh2_bronze_CSV import BronzeSourceBuilderFileCSV
from oci_lh2_bronze_BUCKET_CSV import BronzeSourceBuilderBucketCSV
from NLSOCI.oci_bucket import OCIBucket, OCIEnv
from NLSDATA.oic_lh2_bronze.oci_lh2_bronze import BronzeConfig, BronzeLogger, BronzeSourceBuilder
from NLSOCI import *
from datetime import datetime, timezone
import io
import os
from NLSTOOLS.tool_kits import *

# Creating a test environment to test the above functions
configuration_file = 'C:\Python\projects\LH2Bronze_Loader\config_PCBenjamin.json'
bronze_config = BronzeConfig(configuration_file)
bronze_logger = BronzeLogger(bronze_config)

# Setting up verbose logging if specified in configuration
if bronze_config.get_options().verbose:
    verbose = Logger(bronze_config.get_options().verboselogfile)
else:
    verbose = None

# Instantiating parameters for BronzeSourceBuilder
br_config = bronze_config
src_type = "FILE_CSV"
src_name = "CSV"
src_origin_name = 'C:\Python\projects\LH2Bronze_Loader\\files\\0001000000674520.csv'
src_table_name = "LH2_DATASOURCE_LOADING_DEBUG_BENJAMIN"
src_table_where = ""
src_flag_incr = False
src_date_criteria = ""
src_date_where = ""
src_date_lastupdate = ""
force_encode = False
logger = bronze_logger

# Creating the BronzeSourceBuilder
bronze_builder = BronzeSourceBuilder(br_config, src_type, src_name, src_origin_name, src_table_name, src_table_where,
                                     src_flag_incr, src_date_criteria,src_date_lastupdate, force_encode, logger)

# Creating the BronzeSourceBuilderFile from BronzeSourceBuilder
bronze_source_builder_file = BronzeSourceBuilderFile(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema, bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                 src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

# Creating the BronzeSourceBuilderFileCSV
source=BronzeSourceBuilderFileCSV(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema,
                                  bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                                  src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

# Creating the BronzeSourceBuilderBucketCSV
bucket_source = BronzeSourceBuilderBucketCSV(bronze_builder.bronze_config, bronze_builder.src_name, bronze_builder.src_schema,
                                              bronze_builder.src_table, bronze_builder.src_table_whereclause, bronze_builder.src_flag_incr,
                                              src_date_where, bronze_builder.src_date_lastupdate, bronze_builder.force_encode, logger)

# Recovering files from the specified bucket
bucket_source.recover_files()
