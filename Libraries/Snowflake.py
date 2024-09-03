from Libraries.Database import Database
import os
from dotenv import load_dotenv
from snowflake import connector as sf_connector
from Libraries.Logger import Logger

load_dotenv()

class Snowflake(Database):
    def __init__(self, logger_input):
        #super.__init__()
        self.credentials = {
            'user': os.getenv("SNOWFLAKE_USER"),
            'password': os.getenv("SNOWFLAKE_PASSWORD"),
            'account': os.getenv("SNOWFLAKE_ACCOUNT"),
            'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
            'database': os.getenv("SNOWFLAKE_DATABASE"),
            'schema': os.getenv("SNOWFLAKE_SCHEMA")
        }
        self.logger = logger_input
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        try:
            result = sf_connector.connect(
            user=self.credentials['user'],
            password=self.credentials['password'],
            account=self.credentials['account'],
            warehouse=self.credentials['warehouse'],
            database=self.credentials['database'],
            schema=self.credentials['schema']
            )
            #print(result)
            self.logger.info("Snowflake has been connected successfully")
            return result

        except Exception as e:
            self.logger.error("Failed to connect the snowflake with error: "+str(e))

    def executequery(self, queryString):
        try:
            result = self.cursor.execute(queryString)
            self.logger.info("Query has been executed successfully: "+ str(queryString))
            return result
        except Exception as e:
            self.logger.error("Query could not be executed with error: "+str(e))

    def fetchOne(self, queryString):
        try:
            result = self.cursor.execute(queryString).fetchone()
            self.logger.info("Fetchone has been executed successfully: "+ str(queryString))
            return result
        except Exception as e:
            self.logger.error("FetchOne could not be executed with error: "+str(e))

    def fetchAll(self, queryString):
        try:
            result = self.cursor.execute((queryString)).fetchall()
            self.logger.info("FetchAll has been executed successfully: "+str(queryString))
            return result
        except Exception as e:
            self.logger.error("FetchAll could not be executed with error: "+str(e))
    def disconnect(self):
        try:
            self.connection.close()
            self.logger.info("Connection has been closed successfully")
            self.logger.remove()
        except Exception as e:
            self.logger.error("Could not closed the connection with error: "+str(e))

# x = Snowflake("Snowflake_Test_Log")
# result = (x.executequery("select top 100 * from STORE"))
# for i in result:
#     print(list(result))
# x.disconnect()