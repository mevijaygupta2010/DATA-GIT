import config
import logging
import itertools as it
# -- (> ---------- SECTION=begin_logging -----------------------------
logging.basicConfig(
filename=config.file_name,
level=logging.INFO,
format= '%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s')
# -- <) ---------- END_SECTION ---------------------------------------
date_format =config.DATE_FORMAT
print(date_format)
def create_snow_tables(conn):
    if len(date_format) >0:
        logging.info("Date Format Found.. Altering the session to corresponding Date Format")
        conn.cursor().execute(
            "alter session set DATE_INPUT_FORMAT="+date_format+";")
    
    else:
        logging.info("Date Format Found.. Altering the session to corresponding Date Format")
    # -- (> ----------------------- SECTION=create_table ---------------------
        
    print("\nCreating tables...") 
    logging.info("Creating tables...")  
    with open("C:\Snowflake\Code\Data-Git\src\snowflake\snow_table.txt","r") as f:
        for key,group in it.groupby(f,lambda line: line.startswith('/')):
            
            if not key:
                
                group=list(group)
                # print(group)
                new_group=[x.replace("\n","") for x in group]
                
                query=''.join(new_group)
                print(query)

                logging.info(query)
                conn.cursor().execute(query)
            
           

    # conn.cursor().execute(
    # "create or replace table PRODUCTDIM(    FAMILY             VARCHAR(15)           null    ,    FAMILY_ALIAS       VARCHAR(25)           null    ,    CONSOLIDATION      VARCHAR(1)  null    ,    SKU                VARCHAR(15)           null    ,    SKU_ALIAS          VARCHAR(25)           null    );"
        
    #     )
    # -- <) ---------------------------- END_SECTION -------------------------