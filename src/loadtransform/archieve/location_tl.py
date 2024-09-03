import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger


# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info("########################## Operations in V_LOCATION Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_LOCATION

#creating V_LOCATION temp table
logger_object.info("Creating V_LOCATION temp table: TEMP_LOCATION_LU")
temp_location_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS TEMP_LOCATION_LU(
                    LOCATION_ID INT,
                    LOCATION_NAME VARCHAR(40),
                    STOCKHOLDING_IND VARCHAR(4)
                    )"""
snowflake_object.executequery(temp_location_lu_create_table_query)

logger_object.info("If table already exists then truncating V_LOCATION temp table: TEMP_LOCATION_LU")
temp_location_lu_data_truncate_query = table_truncate_query("TEMP_LOCATION_LU")
snowflake_object.executequery(temp_location_lu_data_truncate_query)

temp_location_lu_data_load_query = """
                        INSERT INTO TEMP_LOCATION_LU
                        (SELECT
                        LOCATION_ID,
                        LOCATION_NAME,
                        STOCKHOLDING_IND
                        FROM V_LOCATION)
                    """

snowflake_object.executequery(temp_location_lu_data_load_query)

#creating V_LOCATION target table
logger_object.info("Creating V_LOCATION load table: DWH_LOCATION_LU")
dwh_location_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS DWH_LOCATION_LU(
                    LOCATION_KEY INT,
                    STOCKHOLDING_IND VARCHAR(4),
                    LOCATION_ID INT,
                    LOCATION_DESC VARCHAR(40),
                    REC_START_TS TIMESTAMP_LTZ,
                    REC_END_TS TIMESTAMP_LTZ,
                    REC_CLOSE_FLG CHAR(1)
                    )"""
snowflake_object.executequery(dwh_location_lu_create_table_query)
logger_object.info("Updating and Inserting V_LOCATION load table: DWH_LOCATION_LU under SCD Type-1 Approach")
dwh_location_lu_load_query_part1 = """
                MERGE INTO DWH_LOCATION_LU AS TG
                USING TEMP_LOCATION_LU AS TM
                ON TG.LOCATION_ID = TM.LOCATION_ID
                WHEN MATCHED AND TG.LOCATION_DESC <> TM.LOCATION_NAME
                THEN UPDATE SET
                TG.LOCATION_KEY = TG.LOCATION_KEY,
                TG.LOCATION_ID = TM.LOCATION_ID,
                TG.STOCKHOLDING_IND = TM.STOCKHOLDING_IND,
                TG.LOCATION_DESC = TM.LOCATION_NAME,
                TG.REC_START_TS = CURRENT_TIMESTAMP,
                TG.REC_END_TS = '2199-12-31 23:59:59.999999',
                REC_CLOSE_FLG = 'N'
                WHEN NOT MATCHED
                THEN INSERT (LOCATION_KEY,LOCATION_ID,STOCKHOLDING_IND,LOCATION_DESC,REC_START_TS,REC_END_TS,REC_CLOSE_FLG)
                VALUES(COALESCE((SELECT MAX(LOCATION_ID) FROM TEMP_LOCATION_LU WHERE LOCATION_ID>=0), 0) + RANK() OVER (ORDER BY LOCATION_ID),
                TM.LOCATION_ID, TM.STOCKHOLDING_IND, TM.LOCATION_NAME, CURRENT_TIMESTAMP, '2199-12-31 23:59:59.999999','N') 
"""
snowflake_object.executequery(dwh_location_lu_load_query_part1)

logger_object.info("Closing LOCATIONS in V_LOCATION load table: DWH_LOCATION_LU under SCD Type-1 Approach")
dwh_location_lu_load_query_part2 = """
                UPDATE DWH_LOCATION_LU
                SET REC_CLOSE_FLG = 'Y',
                REC_END_TS = CURRENT_TIMESTAMP
                WHERE LOCATION_ID NOT IN
                (SELECT DISTINCT LOCATION_ID FROM TEMP_LOCATION_LU)
"""
snowflake_object.executequery(dwh_location_lu_load_query_part2)
logger_object.info("ETL of Table V_LOCATION has been completed successfully!!!")

snowflake_object.disconnect()