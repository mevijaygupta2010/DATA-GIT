import logging
import os
import sys
import config

# -- (> ---------------------- SECTION=import_connector ---------------------
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
# -- <) ---------------------------- END_SECTION ----------------------------

def args_to_properties(args):

        """
        PURPOSE:
            Read the command-line arguments and store them in a dictionary.
            Command-line arguments should come in pairs, e.g.:
                "--user MyUser"
        INPUTS:
            The command line arguments (sys.argv).
        RETURNS:
            Returns the dictionary.
        DESIRABLE ENHANCEMENTS:
            Improve error detection and handling.
        """
        
        connection_parameters = {}

        i = 1
        while i < len(args) - 1:
           
            property_name = args[i]
            # Strip off the leading "--" from the tag, e.g. from "--user".
            property_name = property_name[2:]
            property_value = args[i + 1]
            connection_parameters[property_name] = property_value
            i += 2

        return connection_parameters

def create_connection(argv):

        """
        PURPOSE:
            This gets account identifier and login information from the
            environment variables and command-line parameters, connects to the
            server, and returns the connection object.
        INPUTS:
            argv: This is usually sys.argv, which contains the command-line
                  parameters. It could be an equivalent substitute if you get
                  the parameter information from another source.
        RETURNS:
            A connection.
        """
        # Get the other login info etc. from the command line.
        print(argv)
        print(len(argv))
        if len(argv) < 5:
            msg = "Please pass the following command-line parameters:  "
            msg += "--user <user> --account <account_identifier> "
            logging.error(msg)
        else:
            
            connection_parameters = args_to_properties(argv)
            print(connection_parameters)
            USER = connection_parameters["user"]
            ACCOUNT = connection_parameters["account"]
            # WAREHOUSE = connection_parameters["warehouse"]
            # DATABASE = connection_parameters["database"]
            # SCHEMA = connection_parameters["schema"]
        # If the password is set by both command line and env var, the
        # command-line value takes precedence over (is written over) the
        # env var value.

        # If the password wasn't set either in the environment var or on
        # the command line...
        PASSWORD=config.PASSWORD
        if PASSWORD is None or PASSWORD == '':
            logging.error("The Password is not Provided! Please pass the correct password..")
            sys.exit(-2)
        # -- <) ---------------------------- END_SECTION ---------------------
        print("Connecting...")
        # -- (> ------------------- SECTION=connect_to_snowflake ---------
        connection_flag=None
        try:
            conn = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                # warehouse=WAREHOUSE,
                # database=DATABASE,
                # schema=SCHEMA,
                tracing="ALL"
                )
            logging.info("Connection Established!!")
        # -- <) ---------------------------- END_SECTION -----------------
            connection_flag = conn
            return connection_flag
        except DatabaseError as db_ex:
            logging.error(str(db_ex))
        except Exception as ex:
            logging.error(str(ex))