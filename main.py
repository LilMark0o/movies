from model import *
from time import time

startFromScratch = 1
deleteTables = 0

if __name__ == "__main__":
    time1 = time()
    print("Starting...")
    connection = connect()
    print("Connected")
    if deleteTables:
        deleteAllTables(connection)
    if startFromScratch:
        createTables(connection)
        populate(connection)
    createIndexes(connection)
    showProducts(connection, 5)
    segundosTotales = time() - time1
    horas = segundosTotales // 3600
    minutos = (segundosTotales % 3600) // 60
    print("Tiempo total: {} horas, {} minutos y {} segundos".format(
        horas, minutos, segundosTotales % 60))
    connection.close()
