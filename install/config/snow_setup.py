
import config

def set_up(connection):

        """
        PURPOSE:
            Set up to run a test. You can override this method with one
            appropriate to your test/demo.
        """

        # Create a temporary warehouse, database, and schema.
        create_warehouse_database_and_schema(connection)

def create_warehouse_database_and_schema(conn):

        """
        PURPOSE:
            Create the temporary schema, database, and warehouse that we use
            for most tests/demos.
        """

        # Create a database, schema, and warehouse if they don't already exist.
        print("\nCreating warehouse, database, schema...")
        # -- (> ------------- SECTION=create_warehouse_database_schema -------
        conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS "+ config.WAREHOUSE_NAME)
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS "+ config.DATABASE_NAME)
        conn.cursor().execute("USE DATABASE "+config.DATABASE_NAME)
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS "+config.SCHEMA_NAME)
        # -- <) ---------------------------- END_SECTION ---------------------

        # -- (> --------------- SECTION=use_warehouse_database_schema --------
        conn.cursor().execute("USE WAREHOUSE "+config.WAREHOUSE_NAME)
        conn.cursor().execute("USE DATABASE "+ config.DATABASE_NAME)
        conn.cursor().execute("USE SCHEMA "+config.SCHEMA_NAME)
        # -- <) ---------------------------- END_SECTION ---------------------