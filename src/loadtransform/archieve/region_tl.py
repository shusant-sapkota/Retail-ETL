import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger


# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info("########################## Operations in V_Region Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_REGION

#creating V_REGION temp table
logger_object.info("Creating V_REGION temp table: TEMP_RGN_LU")
temp_rgn_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS TEMP_RGN_LU(
                    CHAIN INT,
                    AREA INT,
                    REGION INT,
                    REGION_NAME VARCHAR(40)
                    )"""
snowflake_object.executequery(temp_rgn_lu_create_table_query)

logger_object.info("If table already exists then truncating V_REGION temp table: TEMP_RGN_LU")
temp_rgn_lu_data_truncate_query = table_truncate_query("TEMP_RGN_LU")
snowflake_object.executequery(temp_rgn_lu_data_truncate_query)

temp_rgn_lu_data_load_query = """
                        INSERT INTO TEMP_RGN_LU
                        (SELECT
                        CHAIN,
                        AREA,
                        REGION,
                        UPPER(REGION_NAME)  as REGION_NAME
                        FROM V_REGION)
                    """

snowflake_object.executequery(temp_rgn_lu_data_load_query)

#creating V_REGION target table
logger_object.info("Creating V_REGION load table: DWH_RGN_LU")
dwh_rgn_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS DWH_RGN_LU(
                    REGION_KEY INT,
                    AREA INT,
                    REGION_ID INT,
                    CHAIN INT,
                    REGION_DESC VARCHAR(40),
                    REC_START_TS TIMESTAMP_LTZ,
                    REC_END_TS TIMESTAMP_LTZ,
                    REC_CLOSE_FLG CHAR(1)
                    )"""
snowflake_object.executequery(dwh_rgn_lu_create_table_query)
logger_object.info("Updating and Inserting V_REGION load table: DWH_RGN_LU under SCD Type-1 Approach")
dwh_rgn_lu_load_query_part1 = """
                MERGE INTO DWH_RGN_LU AS TG
                USING TEMP_RGN_LU AS TM
                ON TG.REGION_ID = TM.REGION
                WHEN MATCHED AND TG.REGION_DESC <> TM.REGION_NAME
                THEN UPDATE SET
                TG.REGION_KEY = TG.REGION_KEY,
                TG.AREA = TM.AREA,
                TG.REGION_ID = TM.REGION,
                TG.CHAIN = TM.CHAIN,
                TG.REGION_DESC = TM.REGION_NAME,
                TG.REC_START_TS = CURRENT_TIMESTAMP,
                TG.REC_END_TS = '2199-12-31 23:59:59.999999',
                REC_CLOSE_FLG = 'N'
                WHEN NOT MATCHED
                THEN INSERT (REGION_KEY,AREA,REGION_ID,CHAIN,REGION_DESC,REC_START_TS,REC_END_TS,REC_CLOSE_FLG)
                VALUES(COALESCE((SELECT MAX(REGION) FROM TEMP_RGN_LU WHERE REGION>=0), 0) + RANK() OVER (ORDER BY REGION),
                TM.AREA, TM.REGION, TM.CHAIN, TM.REGION_NAME, CURRENT_TIMESTAMP, '2199-12-31 23:59:59.999999', 'N') 
"""
snowflake_object.executequery(dwh_rgn_lu_load_query_part1)

logger_object.info("Closing REGIONS in V_REGION load table: DWH_RGN_LU under SCD Type-1 Approach")
dwh_rgn_lu_load_query_part2 = """
                UPDATE DWH_RGN_LU
                SET REC_CLOSE_FLG = 'Y',
                REC_END_TS = CURRENT_TIMESTAMP
                WHERE REGION_ID NOT IN
                (SELECT DISTINCT REGION FROM TEMP_RGN_LU)
"""
snowflake_object.executequery(dwh_rgn_lu_load_query_part2)
logger_object.info("ETL of Table V_REGION has been completed successfully!!!")

snowflake_object.disconnect()