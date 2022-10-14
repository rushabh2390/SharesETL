import psycopg2
import os


class Database(object):
    __connection = None

    @staticmethod
    def getConnection():
        """ Static access method. """
        if Database.__connection == None:
            Database()
        return Database.__connection

    def __init__(self):
        if not Database.__connection:
            database = os.getenv("POSTGRES_DB", default="stock-etl")
            user = os.getenv("POSTGRES_USER", default="stock-etl")
            password = os.getenv("POSTGRES_PASSWORD", default="stock-etl")
            port = os.getenv("POSTGRES_PORT", default=5432)
            host = os.getenv("HOST", default="db")
            Database.__connection = psycopg2.connect(database=database,
                                                   host=host,
                                                   user=user,
                                                   password=password,
                                                   port=port)
        else:
            Database.__connection = self.__connection
