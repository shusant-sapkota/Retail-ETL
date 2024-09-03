from utils.sqls import *
from Libraries.Oracle import Oracle
from Libraries.Snowflake import Snowflake
from Libraries.Logger import Logger
import os
from dotenv import load_dotenv
import csv

load_dotenv()

# #Extraction Table Name
table_name = "V_CHANNEL_LOC"

# Intializing Logger and Logger Details
logger_base_file_name = str.replace(os.path.basename(__file__),'.py','')
logger_object = Logger(logger_base_file_name)

# Fetching the data and headers from oracle database using our Oracle object
logger_object.info("####### CONNECTION --> Starting ORACLE Jobs #######")
channel_object = Oracle(logger_object)
queryString = extraction_query_generator(table_name)
data = channel_object.fetchAll(queryString)
columns = channel_object.fetchHeader(queryString)


# These following three lines of code helps to get the base_path which is pythonProject folder
current_path = os.getcwd()
current_path = os.path.dirname(current_path)
base_path = os.path.dirname(current_path)

# Getting the channel to save the extracted data file in local machine
raw_file_name = get_rawfile_name(table_name)
raw_full_path = os.path.join(base_path, raw_file_name)

# Checking directory if exists or not. If not exists creating the directory
directory_flag = os.path.dirname(raw_full_path)
#print(directory_flag)
if not os.path.exists(directory_flag):
    os.makedirs(directory_flag)

# Writing the extracted Region data in the local machine
with open(raw_full_path,'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing the header row
    csvwriter.writerow(columns)
    # Writing the data rows
    csvwriter.writerows(data)


# Initializing snowflake class to interact with snowflake database
logger_object.info("####### CONNECTION --> Starting SNOWFLAKE Jobs #######")
channel_snowflake = Snowflake(logger_object)


# Creating stage in snowflake if it does not exist
region_stage_creation_query = raw_stage_query_generator()
channel_snowflake.executequery(region_stage_creation_query)

# Uploading the v_region csv file in the above created stage of snowflake.
stage_name = "@RAW_STAGE"
substage_name = f"/{table_name}_RAW"
local_file = f"C:/Users/shusant.sapkota/ETLProject/pythonProject/dump/raw/{str.lower(table_name)}/raw_{str.lower(table_name)}_full_data_from_oracle.csv"
stage_upload_query = raw_stage_upload_query_generator(stage_name, substage_name, local_file)
channel_snowflake.executequery(stage_upload_query)


# Creating Table for the V_REGION raw data
stage_table_creation_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                      LOC_ID INT,
                      CHANNEL_ID INT
                    )"""
channel_snowflake.executequery(stage_table_creation_query)

# Truncating the table if it already there and has some data records
stage_table_truncate_query = table_truncate_query(table_name)
channel_snowflake.executequery(stage_table_truncate_query)

# Loading the Stage Files data into Snowflake Table:
raw_stage_file_name = os.path.basename(local_file)
raw_stage_full_path = f"{stage_name}{substage_name}/{raw_stage_file_name}"
stage_table_load_query = raw_table_load_query_generator(raw_stage_full_path, table_name)
channel_snowflake.executequery(stage_table_load_query)

# print(queryString)
# print(region_table_creation_query)


# Disconnecting the connection with Oracle database:
channel_object.disconnect()
channel_snowflake.disconnect()