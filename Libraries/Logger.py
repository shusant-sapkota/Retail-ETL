import logging
import os
import datetime
from dotenv import load_dotenv
from config.constants import logger_path
from loguru import logger


load_dotenv()

# class Logger:
#     def __init__(self, log_name, fileMode='w'):
#         log_path = logger_path#os.getenv("logger_path")
#         #print(log_path)
#         TIMESTAMP_FORMAT = "%Y_%m_%d_%H_%M_%S"
#
#         if not log_path:
#             print("There is no logger base path in the environment variables")
#         current_ts = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
#
#         log_file_name = f'{log_name}_{current_ts}.log'
#         log_file_name = r'{}'.format(log_file_name)
#         #print(log_file_name)
#         #print(log_path)
#         self.log_file_path = os.path.join(log_path, log_file_name)
#         #self.log_file_path = log_path + log_file_name
#         self.log_file_path = str(self.log_file_path )
#         #print(self.log_file_path)
#         self.logger = self.getLogger(fileMode)
#     def getLogger(self, fileMode):
#         logging.basicConfig(filename=self.log_file_path, format='%(asctime)s %(message)s', filemode=fileMode)
#         logger = logging.getLogger()
#         logger.setLevel(logging.INFO)
#         return logger
#
#     def info(self, message):
#         print(message)
#         self.logger.info(f'[Info]: {message}')
#
#     def error(self, message):
#         self.logger.error(f'[Error]: {message}')
#
#     def close(self):
#         self.logger.handlers.clear()
#
# #x=Logger('testfile')



class Logger:
    def __init__(self, log_name, fileMode='w'):
        log_path = logger_path#os.getenv("logger_path")
        #print(log_path)
        TIMESTAMP_FORMAT = "%Y_%m_%d_%H_%M_%S"
        if not log_path:
            print("There is no logger base path in the environment variables")
        current_ts = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)

        log_file_name = f'{log_name}_{current_ts}.log'
        log_file_name = r'{}'.format(log_file_name)
        #print(log_file_name)
        #print(log_path)

        log_path = os.path.join(log_path, log_name)
        os.makedirs(log_path, exist_ok=True)

        self.log_file_path = os.path.join(log_path, log_file_name)
        #self.log_file_path = log_path + log_file_name
        self.log_file_path = str(self.log_file_path)
        #print(self.log_file_path)
        self.logger = logger
        self.logger.add(self.log_file_path, format="{time} - {message}")


    # def getLogger(self):
    #     print("Hello")
    #     return logger.add(self.log_file_path, format="{time} - {message}")

    def info(self, message):
        self.logger.info(f'[Info]: {message}')
        self.logger.info(100*"~")

    def error(self, message):
        self.logger.error(f'[Error]: {message}')
        self.logger.info(100 * "~")

    def remove(self):
        self.logger.remove()

#x=Logger('testfile')
