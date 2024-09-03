def extraction_query_generator(source_table_name,):
    extract_query = "select * from {}".format(source_table_name)
    return extract_query

#print(extraction_query_generator("V_REGION","L_REGION"))

# This function returns the query that creates the snowflake stage if it doesn't exists
def raw_stage_query_generator():
    region_stage_creation_query = """
    CREATE STAGE IF NOT EXISTS RAW_STAGE
    COMMENT = 'Internal Stage for storing CSV files of raw data from Oracle'
    """
    return region_stage_creation_query

#This function returns the query which is used to upload file from local system to the snowflake stage
def raw_stage_upload_query_generator(stage_name,substage_name,local_file):
    return  f"PUT file://{local_file} {stage_name}{substage_name} AUTO_COMPRESS = FALSE OVERWRITE=TRUE"

# This function returns query to load the data uploaded in snowflake file stage to the snowflake table.
def raw_table_load_query_generator(raw_stage_full_path,table_name):
    return  f"COPY INTO {table_name} FROM {raw_stage_full_path} FILE_FORMAT = (FIELD_DELIMITER = ',' SKIP_HEADER=1)"

# This function returns query to truncate the table
def table_truncate_query(table):
    return f"TRUNCATE {table}"


# This function returns query to upsert(update and insert) dimension to the target table
def upsert_dimension_query(TEMP_TABLE, TARGET_TABLE):
    upsert_query = f"""
    MERGE INTO {TARGET_TABLE}
    USING {TEMP_TABLE}
    ON TARGET
    """


# This function returns the base raw file name.
# This file will save the file locally and is uploaded to the snowflake stage.
def get_rawfile_name(table_name):
    return f'dump\\raw\\{str.lower(table_name)}\\raw_{str.lower(table_name)}_full_data_from_oracle.csv'


# This function returns query to load data from stage table to temp table in snowflake
def stage_to_temp_data_loading_query_generator(stage_table_data_query, temp_table_name):
    return f"INSERT INTO {temp_table_name} ({stage_table_data_query})"


def dwh_inactive_records_closing_query(identifier, temp_table_name, target_table_name):
    return f"""
            UPDATE {target_table_name}
            SET REC_CLOSE_FLG = TRUE,
            REC_END_TS = CURRENT_TIMESTAMP
            WHERE {identifier} NOT IN
            (SELECT DISTINCT {identifier} FROM {temp_table_name})
            """