#Run command py.exe main.py --user VIJAYGUPTA --account kyb40496.prod3.us-west-2.aws BEZBDHG.MXB48701
import logging
import os
import sys

import config
import snow_connect
from snow_setup import set_up as setup_snowflake
from snow_create_table import create_snow_tables as sn_tab
# -- (> ---------------------- SECTION=import_connector ---------------------
# import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
# -- <) ---------------------------- END_SECTION ----------------------------
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY


class python_snowflake:
    """
        PURPOSE:
            This is the Base/Parent class for programs that use the Snowflake
            Connector for Python.
    """
    def __init__(self, p_log_file_name = config.file_name):
        """
        PURPOSE:
            This does any required initialization steps, which in this class is
            basically just turning on logging.
        """
        file_name = p_log_file_name

        # -- (> ---------- SECTION=begin_logging -----------------------------
        logging.basicConfig(
            filename=file_name,
            level=logging.INFO,
            format= '%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s')
        # -- <) ---------- END_SECTION ---------------------------------------

    
    # -- (> ---------------------------- SECTION=main ------------------------
    def main(self, argv):

        """
        PURPOSE:
            Most tests follow the same basic pattern in this main() method:
               * Create a connection.
               * Set up, e.g. use (or create and use) the warehouse, database,
                 and schema.
               * Run the queries (or do the other tasks, e.g. load data).
               * Close the connection.
        """

        # Read the connection parameters (e.g. user ID) from the command line
        # and environment variables, then connect to Snowflake.
        connection = snow_connect.create_connection(argv)
        if connection is not None:
        # Set up anything we need (e.g. a separate schema for the test/demo).
            setup_snowflake(connection)

        # Do the "real work", for example, create a table, insert rows, SELECT
        # from the table, etc.
            
            sn_tab(connection)
            self.load_data(connection)
            # with open("snow_table.txt","r") as f:
            #     for key,group in it.groupby(f,lambda line: line.startswith('"/"')):
            #         if not key:
            #             group=list(group)
            #             query=''.join(group)
            #             print(query)
        # Clean up. In this case, we drop the temporary warehouse, database, and
        # schema.
        #self.clean_up(connection)


            print("\nClosing connection...")
        #--(> ------------------- SECTION=close_connection -----------------
            connection.close()
        # -- <) ---------------------------- END_SECTION ---------------------
        else:
            logging.error("The Connection is not Established! Please see details in the log file....")
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

    def clean_up(self, connection):

        """
        PURPOSE:
            Clean up after a test. You can override this method with one
            appropriate to your test/demo.
        """

        # Create a temporary warehouse, database, and schema.
        self.drop_warehouse_database_and_schema(connection)

    def drop_warehouse_database_and_schema(self, conn):

        """
        PURPOSE:
            Drop the temporary schema, database, and warehouse that we create
            for most tests/demos.
        """

        # -- (> ------------- SECTION=drop_warehouse_database_schema ---------
        conn.cursor().execute("DROP SCHEMA IF EXISTS "+config.SCHEMA_NAME)
        conn.cursor().execute("DROP DATABASE IF EXISTS "+ config.DATABASE_NAME)
        conn.cursor().execute("DROP WAREHOUSE IF EXISTS "+ config.WAREHOUSE_NAME)
        # -- <) ---------------------------- END_SECTION ---------------------
# ----------------------------------------------------------------------------
if __name__ == '__main__':
    pvb = python_snowflake()
    pvb.main(sys.argv)

