
import os

AWS_ACCESS_KEY_ID="AKIAXNSCPZPAEZW6FM6T"
AWS_SECRET_ACCESS_KEY="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"
file_name = 'C:\Snowflake\Code\Data-Git\logs\snowflake_python_connector.log'
os.environ['SNOWSQL_PWD'] ="London$99"
PASSWORD=os.getenv('SNOWSQL_PWD')
WAREHOUSE_NAME = "VJ_WAREHOUSE"
DATABASE_NAME ="TBC"
SCHEMA_NAME="INGESTION"

DATE_FORMAT="'MM/DD/YYYY'"

