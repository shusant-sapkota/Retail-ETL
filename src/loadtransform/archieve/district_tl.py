import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger


# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info("########################## Operations in V_DISTRICT Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_DISTRICT

#creating V_DISTRICT temp table
logger_object.info("Creating V_DISTRICT temp table: TEMP_DISTRICT_LU")
temp_district_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS TEMP_DISTRICT_LU(
                    CHAIN INT,
                    AREA INT,
                    REGION INT,
                    DISTRICT INT,
                    DISTRICT_NAME VARCHAR(40)
                    )"""
snowflake_object.executequery(temp_district_lu_create_table_query)

logger_object.info("If table already exists then truncating V_DISTRICT temp table: TEMP_DISTRICT_LU")
temp_district_lu_data_truncate_query = table_truncate_query("TEMP_DISTRICT_LU")
snowflake_object.executequery(temp_district_lu_data_truncate_query)

temp_district_lu_data_load_query = """
                        INSERT INTO TEMP_DISTRICT_LU
                        (SELECT
                        CHAIN,
                        AREA,
                        REGION,
                        DISTRICT,
                        DISTRICT_NAME
                        FROM V_DISTRICT)
                    """

snowflake_object.executequery(temp_district_lu_data_load_query)

#creating V_DISTRICT target table
logger_object.info("Creating V_DISTRICT load table: DWH_DISTRICT_LU")
dwh_district_lu_create_table_query = """
CREATE TABLE IF NOT EXISTS DWH_DISTRICT_LU(
                    DISTRICT_KEY INT,
                    CHAIN INT,
                    AREA INT,
                    REGION_ID INT,
                    DISTRICT_ID INT,
                    DISTRICT_DESC VARCHAR(40),
                    REC_START_TS TIMESTAMP_LTZ,
                    REC_END_TS TIMESTAMP_LTZ,
                    REC_CLOSE_FLG CHAR(1)
                    )"""
snowflake_object.executequery(dwh_district_lu_create_table_query)
logger_object.info("Updating and Inserting V_DISTRICT load table: DWH_DISTRICT_LU under SCD Type-1 Approach")
dwh_district_lu_load_query_part1 = """
                MERGE INTO DWH_DISTRICT_LU AS TG
                USING TEMP_DISTRICT_LU AS TM
                ON TG.DISTRICT_ID = TM.DISTRICT
                WHEN MATCHED AND TG.DISTRICT_DESC <> TM.DISTRICT_NAME
                THEN UPDATE SET
                TG.DISTRICT_KEY = TG.DISTRICT_KEY,
                TG.AREA = TM.AREA,
                TG.DISTRICT_ID = TM.DISTRICT,
                TG.CHAIN = TM.CHAIN,
                TG.DISTRICT_DESC = TM.DISTRICT_NAME,
                TG.REC_START_TS = CURRENT_TIMESTAMP,
                TG.REC_END_TS = '2199-12-31 23:59:59.999999',
                REC_CLOSE_FLG = 'N'
                WHEN NOT MATCHED
                THEN INSERT (DISTRICT_KEY,AREA,REGION_ID,DISTRICT_ID,CHAIN,DISTRICT_DESC,REC_START_TS,REC_END_TS,REC_CLOSE_FLG)
                VALUES(COALESCE((SELECT MAX(DISTRICT) FROM TEMP_DISTRICT_LU WHERE DISTRICT>=0), 0) + RANK() OVER (ORDER BY DISTRICT),
                TM.AREA, TM.REGION, TM.DISTRICT, TM.CHAIN, TM.DISTRICT_NAME, CURRENT_TIMESTAMP, '2199-12-31 23:59:59.999999', 'N') 
"""
snowflake_object.executequery(dwh_district_lu_load_query_part1)

logger_object.info("Closing DISTRICTS in V_DISTRICT load table: DWH_DISTRICT_LU under SCD Type-1 Approach")
dwh_district_lu_load_query_part2 = """
                UPDATE DWH_DISTRICT_LU
                SET REC_CLOSE_FLG = 'Y',
                REC_END_TS = CURRENT_TIMESTAMP
                WHERE DISTRICT_ID NOT IN
                (SELECT DISTINCT DISTRICT FROM TEMP_DISTRICT_LU)
"""
snowflake_object.executequery(dwh_district_lu_load_query_part2)
logger_object.info("ETL of Table V_DISTRICT has been completed successfully!!!")

snowflake_object.disconnect()