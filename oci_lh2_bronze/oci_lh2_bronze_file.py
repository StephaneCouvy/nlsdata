import pandas as pd
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *

class BronzeSourceBuilderFile(BronzeSourceBuilder):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode,logger):
        super().__init__(br_config, "FILE", src_name, src_origin_name, src_table_name, src_table_where,
                         src_flag_incr, src_date_where, src_date_lastupdate,force_encode,logger)
        self.bronze_table = self.src_name + "_" + self.src_table.replace(" ", "_")
        self.bucket_file_path = self.src_table.replace(" ", "_") + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()
        self.request = "Import File"

    def fetch_source(self,verbose=None):
        try:
            if verbose:
                message = "Extracting data from file {0},{1},{2}".format(self.src_name, self.src_schema, self.src_table)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)
            match self.src_name:
                case "TURKEY":
                    table = pd.read_excel(self.src_schema, sheet_name=self.src_table,skiprows=1)  # adapt with excel table
                case "EXCEL":
                    table = pd.read_excel(self.src_schema, sheet_name=self.src_table, skiprows=0)
                case _:
                    vError = "ERROR Unknown source {0}, extracting data from file {1},{2}".format(self.src_name,self.src_schema,self.src_table)
                    if verbose:
                        verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,log_message='')
                    self.logger.log(error=Exception('Unkown source'), action=vError)
                    return False
            self.df_table_content = table.astype('string')
            res = self.__create_parquet_file__()
            if not res:
                raise Exception("Error creating parquet file")
            self.__update_fetch_row_stats__()
            return True
        except Exception as err:
                vError = "ERROR Extracting from {0}, file {1},{2} : {3}".format(self.src_name,self.src_schema,self.src_table,str(err))
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,log_message=str(err))
                self.logger.log(error=err, action=vError)
                self.__update_fetch_row_stats__()
                return False