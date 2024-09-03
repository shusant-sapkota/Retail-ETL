from abc import ABC, abstractmethod


class Database(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def executequery(self):
        pass

    @abstractmethod
    def fetchOne(self):
        pass

    @abstractmethod
    def fetchAll(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass
