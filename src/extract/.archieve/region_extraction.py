from utils.sqls import *
from Libraries.Oracle import Oracle
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger
import os
from dotenv import load_dotenv
import csv

load_dotenv()

# #Extraction Table Name
table_name = "V_REGION"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

# Fetching the data and headers from oracle database using our Oracle object
logger_object.info("####### CONNECTION --> Starting ORACLE Jobs #######")
region_object = Oracle(logger_object)
queryString = extraction_query_generator(table_name)
region_data = region_object.fetchAll(queryString)
region_columns = region_object.fetchHeader(queryString)


# These following three lines of code helps to get the base_path which is pythonProject folder
current_path = os.getcwd()
current_path = os.path.dirname(current_path)
base_path = os.path.dirname(current_path)

# Getting the location to save the extrated data file in local machine
raw_region_file_name = 'dump\\raw\\v_region\\raw_region_full_data_from_oracle.csv'
raw_region_full_path = os.path.join(base_path, raw_region_file_name)
#print(raw_region_full_path)

# Checking directory if exists or not. If not exists creating the directory
directory_flag = os.path.dirname(raw_region_full_path)
#print(directory_flag)
if not os.path.exists(directory_flag):
    os.makedirs(directory_flag)

# Writing the extracted Region data in the local machine
with open(raw_region_full_path,'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing the header row
    csvwriter.writerow(region_columns)
    # Writing the data rows
    csvwriter.writerows(region_data)

# Initializing snowflake class to interact with snowflake database
logger_object.info("####### CONNECTION --> Starting SNOWFLAKE Jobs #######")
region_snowflake = Snowflake(logger_object)


# Creating stage in snowflake if it does not exist
region_stage_creation_query = """
CREATE STAGE IF NOT EXISTS RAW_STAGE
COMMENT = 'Internal Stage for storing CSV files of raw data from Oracle'
"""
#print(region_stage_creation_query)
region_snowflake.executequery(region_stage_creation_query)


# Uploading the v_region csv file in the above created stage of snowflake.
stage_name = "@RAW_STAGE"
substage_name = "/V_REGION_RAW"
local_file = "C:/Users/shusant.sapkota/ETLProject/pythonProject/dump/raw/v_region/raw_region_full_data_from_oracle.csv"
region_stage_upload_query = f"PUT file://{local_file} {stage_name}{substage_name} AUTO_COMPRESS = FALSE OVERWRITE=TRUE"
region_snowflake.executequery(region_stage_upload_query)


# Creating Table for the V_REGION raw data
region_table_creation_query ="""
                    CREATE TABLE IF NOT EXISTS V_REGION (
                      CHAIN INT,
                      AREA INT,
                      REGION INT,
                      REGION_NAME VARCHAR(40)
                    )"""
region_snowflake.executequery(region_table_creation_query)

# Truncating the table if it already there and has some data records
region_table_truncate_query = table_truncate_query(table_name)
region_snowflake.executequery(region_table_truncate_query)

# Loading the Stage Files data into Snowflake Table:
raw_region_stage_file_name = os.path.basename(local_file)
raw_region_stage_full_path = f"{stage_name}{substage_name}/{raw_region_stage_file_name}"
#print(raw_region_stage_full_path)
region_table_initial_load_query = f"COPY INTO V_REGION FROM {raw_region_stage_full_path} FILE_FORMAT = (FIELD_DELIMITER = ',' SKIP_HEADER=1)"
#print(region_table_initial_load_query)
region_snowflake.executequery(region_table_initial_load_query)

# print(queryString)
# print(region_table_creation_query)
region_snowflake.executequery("commit")

# Disconnecting the connection with Oracle database:
region_object.disconnect()
region_snowflake.disconnect()