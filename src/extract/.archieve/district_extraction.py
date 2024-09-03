from utils.sqls import *
from Libraries.Oracle import Oracle
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger
import os
from dotenv import load_dotenv
import csv

load_dotenv()

# #Extraction Table Name
table_name = "V_DISTRICT"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

# Fetching the data and headers from oracle database using our Oracle object
logger_object.info("####### CONNECTION --> Starting ORACLE Jobs #######")
district_object = Oracle(logger_object)
queryString = extraction_query_generator(table_name)
district_data = district_object.fetchAll(queryString)
district_columns = district_object.fetchHeader(queryString)


# These following three lines of code helps to get the base_path which is pythonProject folder
current_path = os.getcwd()
current_path = os.path.dirname(current_path)
base_path = os.path.dirname(current_path)

# Getting the district to save the extrated data file in local machine
raw_district_file_name = 'dump\\raw\\v_district\\raw_district_full_data_from_oracle.csv'
raw_district_full_path = os.path.join(base_path, raw_district_file_name)
#print(raw_district_full_path)



# Checking directory if exists or not. If not exists creating the directory
directory_flag = os.path.dirname(raw_district_full_path)
#print(directory_flag)
if not os.path.exists(directory_flag):
    os.makedirs(directory_flag)

# Writing the extracted Region data in the local machine
with open(raw_district_full_path,'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing the header row
    csvwriter.writerow(district_columns)
    # Writing the data rows
    csvwriter.writerows(district_data)


# Initializing snowflake class to interact with snowflake database
logger_object.info("####### CONNECTION --> Starting SNOWFLAKE Jobs #######")
district_snowflake = Snowflake(logger_object)


# Creating stage in snowflake if it does not exist
region_stage_creation_query = raw_stage_query_generator()
district_snowflake.executequery(region_stage_creation_query)

# Uploading the v_region csv file in the above created stage of snowflake.
stage_name = "@RAW_STAGE"
substage_name = "/V_DISTRICT_RAW"
local_file = "C:/Users/shusant.sapkota/ETLProject/pythonProject/dump/raw/v_district/raw_district_full_data_from_oracle.csv"
district_stage_upload_query = raw_stage_upload_query_generator(stage_name, substage_name, local_file)
district_snowflake.executequery(district_stage_upload_query)


# Creating Table for the V_REGION raw data
district_table_creation_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                      CHAIN INT,
                      AREA INT,
                      REGION INT,
                      DISTRICT INT,
                      DISTRICT_NAME VARCHAR(40)
                    )"""
district_snowflake.executequery(district_table_creation_query)

# Truncating the table if it already there and has some data records
district_table_truncate_query = table_truncate_query(table_name)
district_snowflake.executequery(district_table_truncate_query)

# Loading the Stage Files data into Snowflake Table:
raw_district_stage_file_name = os.path.basename(local_file)
raw_district_stage_full_path = f"{stage_name}{substage_name}/{raw_district_stage_file_name}"
#print(raw_district_stage_full_path)
#region_table_initial_load_query = f"COPY INTO V_REGION FROM {raw_region_stage_full_path} FILE_FORMAT = (FIELD_DELIMITER = ',' SKIP_HEADER=1)"
district_raw_table_load_query = raw_table_load_query_generator(raw_district_stage_full_path, table_name)
#print(district_raw_table_load_query)
district_snowflake.executequery(district_raw_table_load_query)

# print(queryString)
# print(region_table_creation_query)


# Disconnecting the connection with Oracle database:
district_object.disconnect()
district_snowflake.disconnect()