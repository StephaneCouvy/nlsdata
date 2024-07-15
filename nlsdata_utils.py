from nlstools.tool_kits import *

def format_log_filename(p_filename, p_prefix_name):
    # create log file based on : logfile_name_template + _ + Now Date + _ + processus PID + . + extension
    # to have an unique log file name is several process are running in parallel
    (vDirName,vFileName) = os.path.split(p_filename)
    vSplitFile = os.path.splitext(vFileName)
    vPid = os.getpid()
    vNow = datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')
    vNewFileName = '{}_{}_{}_{}{}'.format(p_prefix_name, vSplitFile[0], vNow, vPid, vSplitFile[1])
    vNewFullFileName = os.path.join(vDirName, vNewFileName)
    return vNewFullFileName

def format_temporary_tablename(p_tablename):
    # Create running/temporary List Datasource loading table.
    # Insert list of tables to import
    hostname = socket.gethostname()
    pid = os.getpid()
    # Temporary table name
    v_temp_table = p_tablename.upper() + '_TEMP_' + str(hostname) + "_" + str(pid)
    v_temp_table = v_temp_table.replace('-','_')
    
    return v_temp_table
