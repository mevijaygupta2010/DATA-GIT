# from deployment_script import import_infa_objects
import logging
import getpass
import runpy
import sys
import argparse
import os
import datetime
from datetime import datetime
import subprocess
from pathlib import Path
sys.path.insert(0, 'C:\Snowflake\Code\Data-Git\install\config')
import config
import snow_connect
from snow_setup import set_up as setup_snowflake
from snow_create_table import create_snow_tables as sn_tab
# -- (> ---------------------- SECTION=import_connector ---------------------
# import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
# -- <) ---------------------------- END_SECTION ---------------------------

# Parsing command line arguments
def parseCommandLineAruguments():
    try:
        parser = argparse.ArgumentParser(
            description='To generate Deployment Script. Process arguments need 3 mandatory and 1 optional.')
        parser.add_argument('deploy_version', help='Need deploy folder name')
        parser.add_argument(
            'run_command', help='Need run command (g mandatory, dmi optional)')
        parser.add_argument('env_detail', help='Need Deployment Env Detail')
        parser.add_argument('--s', help='Optional scrum folder name')
       
        return parser
    except Exception as e:
        print('Error in parsing commandline arguments')

# Interpreting command line arguments
try:
    parser = parseCommandLineAruguments()
    args = parser.parse_args()
    deploy_version = str(args.deploy_version)
    run_command = args.run_command.lower().strip()
    env_detail=args.env_detail.lower().strip()
    scrum_name = args.s

except:
    parser.print_help()
    sys.exit(0)


def create_log(log_id, log_file_name, object_name):
    create_log_rc = 99
    logger = None
    try:
        deploy_version_log = deploy_version.replace('.', '')
        dt = datetime.now().strftime("%Y%m%d%H%M%S")
        edw_log_dir = "C:\Snowflake\Code\Data-Git\logs\deploy_"
        logger = logging.getLogger(log_id)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s:%(name)s:%(levelname)s:%(message)s\n')
        file_handler = logging.FileHandler(
            filename=edw_log_dir + log_file_name + '_' + deploy_version_log + '_' + dt + '.log', mode='w')
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        logger.info('log file name:{}'.format(file_handler.baseFilename))
        create_log_rc = 0
        file_name = file_handler.baseFilename
    except Exception as e:
        print("Error in Creating log object: ", str(e))
    return create_log_rc, logger, file_name




# Start logging
log_id = deploy_version
log_id_err = deploy_version + "_ERR"
jobName = 'deploy'
create_log_rc, log, file_name = create_log(
        log_id, os.path.basename(__file__).split('.')[0], jobName)
create_log_rc, log_err, file_name_err = create_log(
        log_id_err, os.path.basename(__file__).split('.')[0]+'_err', jobName+'_err')
if create_log_rc != 0:
    log.error("Create Logs Failed and return code is ", str(create_log_rc))
    close_log(log_id)
    sys.exit(1)

release_sprint=deploy_version
log.info("Starting the Deployment Process....")
log.info("\ndeploy version: "+deploy_version+"\nRun Command: "+run_command+"\nEnvironment Details: "+env_detail)
# get_env_var_rc = get_env_var()
# deployment_script_branch = os.environ.get('deployment_script_branch')
# dw_ins = os.environ.get('dw_ins')
# release_sprint='2023.01'
with open('deploy_report.txt') as report_file:
    lines=report_file.readlines()
report_file.close()
deploy_list=[line for line in lines if release_sprint in line]
log.info(deploy_list)
for i in range(len(deploy_list)):
    if (env_detail=="qa"):
        deployment_status=deploy_list[i].split("|")[9].strip()
    elif (env_detail=="uat"):
        deployment_status=deploy_list[i].split("|")[10].strip()
    elif (env_detail=="prod"):
        deployment_status=deploy_list[i].split("|")[11].strip()
    else:
        print(" Please check Environment details!")
        sys.exit(0)
log.info("\nRelease Sprint: "+release_sprint+"\nDeployment Status for: "+deploy_list[i].split("|")[1].strip()+ " is "+deployment_status)


sys.argv = ['deploy_dw.py','--user','VIJAYGUPTA','--account','kyb40496.prod3.us-west-2.aws']
connection = snow_connect.create_connection(sys.argv)
if connection is not None:
    setup_snowflake(connection)
    log.info("The Snowflake Connection is Established....")
    sn_tab(connection)
    # load_data(connection)
    print("\nClosing connection...")
    #--(> ------------------- SECTION=close_connection -----------------
    connection.close()
    # -- <) ---------------------------- END_SECTION ---------------------
else:
    log.info("The Connection is not Established! Please see details in the log file....")
    print("The Connection is not Established! Please see details in the log file....")
    # -- <) ---------------------------- END_SECTION=main --------------------

def load_data(self, conn):
        """
            INPUTS:
            conn is a Connection object returned from snowflake.connector.connect().
        """
        
        
        print("\nCreating File Format...")
        # -- (> ----------------------- SECTION=create File Format ---------------------
        conn.cursor().execute(
            "create or replace file format "+config.DATABASE_NAME+"."+config.SCHEMA_NAME+".CSV_FORMAT "
            "type='CSV' "
            "compression='AUTO' "
            "FIELD_DELIMITER=',' "
            "SKIP_HEADER=1;"
            )
        # -- <) ---------------------------- END_SECTION -------------------------
        print("\nCreating Stage...")
        # -- (> ----------------------- SECTION=create External Stage ---------------------
      
        conn.cursor().execute("""
        create or replace STAGE TBC.INGESTION.TBC_STAGE 
        URL='s3://vj-snowflake-training' 
        credentials=(
            AWS_KEY_ID='{AWS_KEY_ID}'
            AWS_SECRET_KEY='{AWS_SECRET_KEY}');
        """.format(AWS_KEY_ID=AWS_ACCESS_KEY_ID ,
           AWS_SECRET_KEY=AWS_SECRET_ACCESS_KEY 
        )
        )
        # -- <) ---------------------------- END_SECTION -------------------------

        print("\nLoading Data..")
        # -- (> ----------------------- SECTION=Loading Data ---------------------
        conn.cursor().execute(
        "copy into TBC.INGESTION.FAMILY from @TBC.INGESTION.TBC_STAGE/TBC/Family.csv "
        "file_format = TBC.INGESTION.CSV_FORMAT;"
        )

        conn.cursor().execute(
        "copy into TBC.INGESTION.PRODUCTDIM from @TBC.INGESTION.TBC_STAGE/TBC/PRODUCTDIM.csv "
        "file_format = TBC.INGESTION.CSV_FORMAT;"
        )

        # -- <) ---------------------------- END_SECTION -------------------------
        print("\nSelecting from test_table...")
        # -- (> ----------------------- SECTION=querying_data --------------------
        cur = conn.cursor()
        try:
            cur.execute("SELECT FAMILY, FAMILY_ALIAS FROM TBC.INGESTION.Family")
            for (FAMILY, FAMILY_ALIAS) in cur:
                print('{0}, {1}'.format(FAMILY, FAMILY_ALIAS))
        finally:
            cur.close()
    # -- <) ---------------------------- END_SECTION -------------------------
