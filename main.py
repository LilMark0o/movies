from model import *
from time import time

startFromScratch = True
deleteTables = True

if __name__ == "__main__":
    connection = connect()
    if deleteTables:
        deleteAllTables(connection)
    if startFromScratch:
        createTables(connection)
        populate(connection)

    connection.close()
