import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger


# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info("########################## Operations in V_AREA Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_AREA

#creating V_AREA temp table
logger_object.info("Creating V_AREA temp table: TEMP_AREA_LU")
temp_area_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS TEMP_AREA_LU(
                    CHAIN INT,
                    AREA INT,
                    AREA_NAME VARCHAR(40)
                    )"""
snowflake_object.executequery(temp_area_lu_create_table_query)

logger_object.info("If table already exists then truncating V_AREA temp table: TEMP_AREA_LU")
temp_area_lu_data_truncate_query = table_truncate_query("TEMP_AREA_LU")
snowflake_object.executequery(temp_area_lu_data_truncate_query)

temp_area_lu_data_load_query = """
                        INSERT INTO TEMP_AREA_LU
                        (SELECT
                        CHAIN,
                        AREA,
                        AREA_NAME
                        FROM V_AREA)
                    """

snowflake_object.executequery(temp_area_lu_data_load_query)

#creating V_AREA target table
logger_object.info("Creating V_AREA load table: DWH_AREA_LU")
dwh_area_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS DWH_AREA_LU(
                    AREA_KEY INT,
                    CHAIN INT,
                    AREA_ID INT,
                    AREA_DESC VARCHAR(40),
                    REC_START_TS TIMESTAMP_LTZ,
                    REC_END_TS TIMESTAMP_LTZ,
                    REC_CLOSE_FLG CHAR(1)
                    )"""
snowflake_object.executequery(dwh_area_lu_create_table_query)
logger_object.info("Updating and Inserting V_AREA load table: DWH_AREA_LU under SCD Type-1 Approach")
dwh_area_lu_load_query_part1 = """
                MERGE INTO DWH_AREA_LU AS TG
                USING TEMP_AREA_LU AS TM
                ON TG.AREA_ID = TM.AREA
                WHEN MATCHED AND TG.AREA_DESC <> TM.AREA_NAME
                THEN UPDATE SET
                TG.AREA_KEY = TG.AREA_KEY,
                TG.AREA_ID = TM.AREA,
                TG.CHAIN = TM.CHAIN,
                TG.AREA_DESC = TM.AREA_NAME,
                TG.REC_START_TS = CURRENT_TIMESTAMP,
                TG.REC_END_TS = '2199-12-31 23:59:59.999999',
                REC_CLOSE_FLG = 'N'
                WHEN NOT MATCHED
                THEN INSERT (AREA_KEY,AREA_ID,CHAIN,AREA_DESC,REC_START_TS,REC_END_TS,REC_CLOSE_FLG)
                VALUES(COALESCE((SELECT MAX(AREA) FROM TEMP_AREA_LU WHERE AREA>=0), 0) + RANK() OVER (ORDER BY AREA),
                TM.AREA, TM.CHAIN, TM.AREA_NAME, CURRENT_TIMESTAMP, '2199-12-31 23:59:59.999999', 'N') 
"""
snowflake_object.executequery(dwh_area_lu_load_query_part1)

logger_object.info("Closing AREAS in V_AREA load table: DWH_AREA_LU under SCD Type-1 Approach")
dwh_area_lu_load_query_part2 = """
                UPDATE DWH_AREA_LU
                SET REC_CLOSE_FLG = 'Y',
                REC_END_TS = CURRENT_TIMESTAMP
                WHERE AREA_ID NOT IN
                (SELECT DISTINCT AREA FROM TEMP_AREA_LU)
"""
snowflake_object.executequery(dwh_area_lu_load_query_part2)
logger_object.info("ETL of Table V_AREA has been completed successfully!!!")

snowflake_object.disconnect()