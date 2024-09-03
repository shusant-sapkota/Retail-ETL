import os
from utils.sqls import *
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger

stage_table_name = "V_SALES_TXN_DAY"
temp_table_name = "TEMP_SALES_TXN_DAY"
target_table_name = "DWH_SALES_TXN_MASTER"
identifier = "TXN_ID"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

logger_object.info(f"########################## Operations in {target_table_name} Table ##########################")

# Initializing Snowflake Object for Transformation and Loading
snowflake_object = Snowflake(logger_object)

## V_REGION

#creating V_REGION temp table
logger_object.info("Creating Temp Table: {}".format(temp_table_name))
temp_lu_create_table_query = f"""
CREATE TABLE IF NOT EXISTS {temp_table_name}(
                    TXN_DT DATE,
                    BUSINESS_DT DATE,
                    TXN_ID NUMBER(38,0),
                    ITEM_ID NUMBER(38,0),
                    LOC_ID NUMBER(38,0),
                    TXN_TYP VARCHAR(5),
                    SALES_AMOUNT FLOAT,
                    SALES_QUANTITY NUMBER(38,0),
                    SALES_COST FLOAT,
                    SALES_DISCOUNT FLOAT
                    )"""
snowflake_object.executequery(temp_lu_create_table_query)

logger_object.info("If table already exists then truncating temp table: {}".format(temp_table_name))
temp_lu_data_truncate_query = table_truncate_query(temp_table_name)
snowflake_object.executequery(temp_lu_data_truncate_query)

stage_table_data_query = extraction_query_generator(stage_table_name)
temp_lu_data_load_query = stage_to_temp_data_loading_query_generator(stage_table_data_query, temp_table_name)

snowflake_object.executequery(temp_lu_data_load_query)

#creating V_REGION target table
logger_object.info(f"Creating Target Table: {target_table_name}")
dwh_lu_create_table_query = f"""
CREATE TABLE IF NOT EXISTS {target_table_name}(
                    TXN_DT TIMESTAMP_NTZ(9),
                    BUSINESS_DT TIMESTAMP_NTZ(9),
                    DAY_KEY TIMESTAMP_NTZ(9),
                    TXN_ID NUMBER(38,0),
                    TXN_KEY NUMBER(38,0),
                    ITEM_ID NUMBER(38,0),
                    TXN_LINE_ITEM_KEY NUMBER(38,0),
                    LOC_ID NUMBER(38,0),
                    LOC_KEY NUMBER(38,0),
                    TXN_TYP VARCHAR(5),
                    SALES_AMOUNT FLOAT,
                    SALES_QUANTITY FLOAT,
                    SALES_COST FLOAT,
                    SALES_DISCOUNT FLOAT
                    )"""

snowflake_object.executequery(dwh_lu_create_table_query)
logger_object.info("Updating and Inserting Target Table: {} under SCD Type-1 Approach".format(target_table_name))
dwh_lu_load_query_part1 = f"""
                MERGE INTO {target_table_name} AS TG
                USING {temp_table_name} AS TM
                ON TG.TXN_ID||TG.ITEM_ID = TM.TXN_ID||TM.ITEM_ID
                WHEN MATCHED AND ((TG.TXN_DT <> TM.TXN_DT) OR (TG.BUSINESS_DT <> TM.BUSINESS_DT) OR (TG.LOC_ID <> TM.LOC_ID) OR (TG.TXN_TYP <> TM.TXN_TYP) OR (TG.SALES_AMOUNT <> TM.SALES_AMOUNT) OR (TG.SALES_QUANTITY <> TM.SALES_QUANTITY) OR (TG.SALES_COST <> TM.SALES_COST) OR (TG.SALES_DISCOUNT <> TM.SALES_DISCOUNT))
                THEN UPDATE SET
                TG.TXN_DT = TM.TXN_DT,
                TG.BUSINESS_DT = TM.BUSINESS_DT,
                TG.LOC_ID = TM.LOC_ID,
                TG.TXN_TYP = TM.TXN_TYP,
                TG.SALES_AMOUNT = TM.SALES_AMOUNT,
                TG.SALES_QUANTITY = TM.SALES_QUANTITY,
                TG.SALES_COST = TM.SALES_COST,
                TG.SALES_DISCOUNT = TM.SALES_DISCOUNT
                WHEN NOT MATCHED
                THEN INSERT (TXN_DT,BUSINESS_DT,DAY_KEY,TXN_ID,TXN_KEY,ITEM_ID,TXN_LINE_ITEM_KEY,LOC_ID,LOC_KEY,TXN_TYP,SALES_AMOUNT,SALES_QUANTITY,SALES_COST,SALES_DISCOUNT)
                VALUES(TM.TXN_DT,TM.BUSINESS_DT,TM.TXN_DT,TM.TXN_ID,COALESCE((SELECT MAX(TXN_ID) FROM TEMP_SALES_TXN_DAY WHERE TXN_ID>=0), 0) + RANK() OVER (ORDER BY TXN_ID),
                TM.ITEM_ID,COALESCE((SELECT MAX(ITEM_ID) FROM TEMP_SALES_TXN_DAY WHERE ITEM_ID>=0), 0) + DENSE_RANK() OVER (ORDER BY ITEM_ID),TM.LOC_ID,COALESCE((SELECT MAX(LOC_ID) FROM TEMP_SALES_TXN_DAY WHERE LOC_ID>=0), 0) + RANK() OVER (ORDER BY LOC_ID), TM.TXN_TYP,TM.SALES_AMOUNT,TM.SALES_QUANTITY,TM.SALES_COST,TM.SALES_DISCOUNT)
                """

snowflake_object.executequery(dwh_lu_load_query_part1)

#logger_object.info(f"Closing Inactives of: {target_table_name} under SCD Type-1 Approach")
#dwh_lu_load_query_part2 = dwh_inactive_records_closing_query(identifier, temp_table_name, target_table_name)
#snowflake_object.executequery(dwh_lu_load_query_part2)
logger_object.info(f"ETL of Table: {target_table_name}  has been completed successfully!!!")

snowflake_object.disconnect()