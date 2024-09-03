from utils.sqls import *
from Libraries.Oracle import Oracle
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger
import os
from dotenv import load_dotenv
import csv

load_dotenv()

# #Extraction Table Name
table_name = "V_AREA"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

# Fetching the data and headers from oracle database using our Oracle object
logger_object.info("####### CONNECTION --> Starting ORACLE Jobs #######")
area_object = Oracle(logger_object)
queryString = extraction_query_generator(table_name)
area_data = area_object.fetchAll(queryString)
area_columns = area_object.fetchHeader(queryString)


# These following three lines of code helps to get the base_path which is pythonProject folder
current_path = os.getcwd()
current_path = os.path.dirname(current_path)
base_path = os.path.dirname(current_path)

# Getting the area to save the extrated data file in local machine
raw_area_file_name = 'dump\\raw\\v_area\\raw_area_full_data_from_oracle.csv'
raw_area_full_path = os.path.join(base_path, raw_area_file_name)
#print(raw_area_full_path)



# Checking directory if exists or not. If not exists creating the directory
directory_flag = os.path.dirname(raw_area_full_path)
#print(directory_flag)
if not os.path.exists(directory_flag):
    os.makedirs(directory_flag)

# Writing the extracted Region data in the local machine
with open(raw_area_full_path,'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing the header row
    csvwriter.writerow(area_columns)
    # Writing the data rows
    csvwriter.writerows(area_data)


# Initializing snowflake class to interact with snowflake database
logger_object.info("####### CONNECTION --> Starting SNOWFLAKE Jobs #######")
area_snowflake = Snowflake(logger_object)


# Creating stage in snowflake if it does not exist
region_stage_creation_query = raw_stage_query_generator()
#print(region_stage_creation_query)
area_snowflake.executequery(region_stage_creation_query)

# Uploading the v_region csv file in the above created stage of snowflake.
stage_name = "@RAW_STAGE"
substage_name = "/V_AREA_RAW"
local_file = "C:/Users/shusant.sapkota/ETLProject/pythonProject/dump/raw/v_area/raw_area_full_data_from_oracle.csv"
area_stage_upload_query = raw_stage_upload_query_generator(stage_name, substage_name, local_file)
#print(area_stage_upload_query)
area_snowflake.executequery(area_stage_upload_query)


# Creating Table for the V_REGION raw data
area_table_creation_query = """
                    CREATE TABLE IF NOT EXISTS V_AREA (
                      CHAIN INT,
                      AREA INT,
                      AREA_NAME VARCHAR(40)
                    )"""
area_snowflake.executequery(area_table_creation_query)

# Truncating the table if it already there and has some data records
area_table_truncate_query = table_truncate_query(table_name)
area_snowflake.executequery(area_table_truncate_query)


# Loading the Stage Files data into Snowflake Table:
raw_area_stage_file_name = os.path.basename(local_file)
raw_area_stage_full_path = f"{stage_name}{substage_name}/{raw_area_stage_file_name}"
#print(raw_area_stage_full_path)
#region_table_initial_load_query = f"COPY INTO V_REGION FROM {raw_region_stage_full_path} FILE_FORMAT = (FIELD_DELIMITER = ',' SKIP_HEADER=1)"
area_raw_table_load_query = raw_table_load_query_generator(raw_area_stage_full_path, table_name)
#print(area_raw_table_load_query)
area_snowflake.executequery(area_raw_table_load_query)

# print(queryString)
# print(region_table_creation_query)


# Disconnecting the connection with Oracle database:
area_object.disconnect()
area_snowflake.disconnect()




