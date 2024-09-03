from Libraries.Database import Database
import os
from dotenv import load_dotenv
import oracledb
from Libraries.Logger import Logger

load_dotenv('.env')


class Oracle(Database):
    def __init__(self, logger_input):
        #super.__init__()
        self.oracleThickLoading()
        self.credentials = {
            'user': os.getenv("ORACLE_USERNAME"),
            'password': os.getenv("ORACLE_PASSWORD"),
            'dsn': os.getenv("ORACLE_DSN")
        }
        self.logger = logger_input
        self.connection = self.connect()
        self.cursor = self.connection.cursor()
    def oracleThickLoading(self):
        defaultPath = os.getenv("ORACLE_CLIENT_PATH")
        oracledb.init_oracle_client(lib_dir=defaultPath)
    def connect(self):
        try:
            connection = oracledb.connect(**self.credentials)
            self.logger.info("Connection has been established sucessfully")
            return connection
        except Exception as e:
            self.logger.error(str(e))
    def executequery(self, queryString):
        try:
            self.cursor.execute(queryString)
            #print("Query has been executed successfully successfully !")
            self.logger.info("Following query has been executed successfully: [[" + queryString + "]]")
            return self.cursor.execute(queryString)
        except Exception as e:
            #print("Query error, please enter valid query")
            self.logger.info("Unable to execute the query with following error message: "+str(e))

    def fetchOne(self, queryString):
        try:
            result = self.cursor.execute(queryString).fetchone()
            self.logger.info("Fetching single data SUCCESSFUL")
            return result
        except Exception as e:
            self.logger.error("Fetching single data UNSUCCESSFUL with error: "+str(e))


    def fetchAll(self,queryString):
        try:
            result = self.cursor.execute(queryString).fetchall()
            self.logger.info("Fetching ALL data SUCCESSFUL")
            return result
        except Exception as e:
            self.logger.error("Fetching ALL data UNSUCCESSFUL with error: " + str(e))

    def fetchHeader(self, queryString):
        try:
            column = self.cursor.description
            column = [col[0] for col in column]
            self.logger.info("Column Fetching has been SUCCESSFUL")
            return column
        except Exception as e:
            self.logger.error("Column Fetching has been SUCCESSFUL with error: " + str(e))
    def disconnect(self):
        try:
            self.connection.close()
            self.logger.info("Connection has been closed successfully")
            self.logger.remove()
            #print("Connection has been closed successfully !")
        except oracledb.Error as e:
            self.logger.error("Could not closed the connection with error: " + str(e))

# logger_inp = Logger('test')
# x = Oracle(logger_inp)
# result = x.fetchAll("select * FROM RMS14.V_REGION")
# print(result)

#result = x.executequery("select * FROM RMS14.V_REGION")
#result = x.fetchOne("select * FROM RMS14.V_REGION")
#result = x.fetchAll("select * FROM RMS14.V_REGION")
#print(result)
#result = x.executequery

#x.disconnect()