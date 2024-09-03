import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger

stage_table_name = "V_REGION_LOC"
temp_table_name = "TEMP_REGION_LOC_LU"
target_table_name = "DWH_REGION_LOC_LU"
identifier = "REGION_ID"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info(f"########################## Operations in {target_table_name} Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_REGION_LOC

#creating V_REGION_LOC temp table
logger_object.info("Creating Temp Table: {}".format(temp_table_name))
temp_lu_create_table_query = f"""
CREATE TABLE IF NOT EXISTS {temp_table_name}(
                    LOC_ID INT,
                    REGION_ID INT
                    )"""
snowflake_object.executequery(temp_lu_create_table_query)

logger_object.info("If table already exists then truncating temp table: {}".format(temp_table_name))
temp_lu_data_truncate_query = table_truncate_query(temp_table_name)
snowflake_object.executequery(temp_lu_data_truncate_query)

stage_table_data_query = extraction_query_generator(stage_table_name)
temp_lu_data_load_query = stage_to_temp_data_loading_query_generator(stage_table_data_query, temp_table_name)

snowflake_object.executequery(temp_lu_data_load_query)

#creating V_REGION_LOC target table
logger_object.info(f"Creating Target Table: {target_table_name}")
dwh_lu_create_table_query = f"""
CREATE TABLE IF NOT EXISTS {target_table_name}(
                    REGION_LOC_KEY INT,
                    LOC_ID INT,
                    REGION_ID INT,
                    REC_START_TS TIMESTAMP_LTZ,
                    REC_END_TS TIMESTAMP_LTZ,
                    REC_CLOSE_FLG BOOLEAN
                    )"""

snowflake_object.executequery(dwh_lu_create_table_query)
logger_object.info("Updating and Inserting Target Table: {} under SCD Type-1 Approach".format(target_table_name))
dwh_lu_load_query_part1 = f"""
                MERGE INTO {target_table_name} AS TG
                USING {temp_table_name} AS TM
                ON TG.{identifier} = TM.{identifier}
                WHEN MATCHED AND ((TG.LOC_ID <> TM.LOC_ID))
                THEN UPDATE SET
                TG.LOC_ID = TM.LOC_ID,
                TG.REC_START_TS = CURRENT_TIMESTAMP,
                TG.REC_END_TS = '2199-12-31 23:59:59.999999'
                WHEN NOT MATCHED
                THEN INSERT (REGION_LOC_KEY,LOC_ID,REGION_ID,REC_START_TS,REC_END_TS,REC_CLOSE_FLG)
                VALUES(COALESCE((SELECT MAX(REGION_ID) FROM {temp_table_name} WHERE REGION_ID>=0), 0) + RANK() OVER (ORDER BY REGION_ID),
                TM.LOC_ID, TM.REGION_ID,CURRENT_TIMESTAMP, '2199-12-31 23:59:59.999999',FALSE) 
                """

snowflake_object.executequery(dwh_lu_load_query_part1)

logger_object.info(f"Closing Inactives of: {target_table_name} under SCD Type-1 Approach")
dwh_lu_load_query_part2 = dwh_inactive_records_closing_query(identifier, temp_table_name, target_table_name)
snowflake_object.executequery(dwh_lu_load_query_part2)
logger_object.info(f"ETL of Table: {target_table_name}  has been completed successfully!!!")

snowflake_object.disconnect()