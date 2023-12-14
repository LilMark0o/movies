from pprint import pprint
import mysql.connector

import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd


def connect():
    load_dotenv()
    host = str(os.getenv("HOST"))
    database = str(os.getenv("DATABASE"))
    user = str(os.getenv("USERDB"))
    password = str(os.getenv("PASSWORD"))
    port = 3306
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return connection


def close(connection, cursor):
    if 'cursor' in locals() and cursor.is_open():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Connection closed")


def getColumnsActors():
    columns = [
        "names VARCHAR(255)"
    ]
    return columns


def getColumnsGenders():
    columns = [
        "genre VARCHAR(255)"
    ]
    return columns


def getColumnsMovies():
    columns = [
        "names VARCHAR(255)",
        "date_x DATE",
        "score FLOAT",
        "genre VARCHAR(255)",
        "overview TEXT",
        "budget_x BIGINT",
        "revenue BIGINT",
        "country VARCHAR(50)"
    ]
    return columns


def createTable(connection, table_name, columns):
    # Construct the SQL query based on the provided columns
    create_table_query = f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        )
    """

    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table '{table_name}' created successfully")


def createMany2Many(connection):
    createTable(connection, "moviesActors", ["movie_id INT", "actor_id INT", "FOREIGN KEY (movie_id) REFERENCES {}(id)".format(
        "movies"), "FOREIGN KEY (actor_id) REFERENCES {}(id)".format("actors")])

    # Create the movies_genres table with foreign keys
    createTable(connection, "moviesGenres", ["movie_id INT", "genre_id INT", "FOREIGN KEY (movie_id) REFERENCES {}(id)".format(
        "movies"), "FOREIGN KEY (genre_id) REFERENCES {}(id)".format("genre")])


def deleteTable(connection, table_name):
    delete_table_query = f"""
        DROP TABLE {table_name}
        """
    cursor = connection.cursor()
    cursor.execute(delete_table_query)
    connection.commit()
    print(f"Table '{table_name}' dropped successfully")


def readData(dataSize="small"):
    df = pd.read_csv(f"imdb_movies_{dataSize}.csv")
    return df


def differentActors(df):
    differentNames = []
    for data in df["crew"]:
        # Check if data is not NaN
        if pd.notna(data):
            for person in data.split(","):
                differentNames.append(
                    person.strip().replace("'", " ").replace('"', " "))
    return list(set(differentNames))


def actorsInMovies(df):
    actorsInMovies = dict()
    for index, row in df.iterrows():
        if pd.notna(row['crew']):
            for actor in row['crew'].split(","):
                actorFine = actor.strip().replace("'", " ").replace('"', " ")
                movie = row['names']
                if movie in actorsInMovies:
                    actorsInMovies[movie].append(actorFine)
                else:
                    actorsInMovies[movie] = [actorFine]
    return actorsInMovies


def genresInMovies(df):
    genresInMovies = dict()
    for index, row in df.iterrows():
        if pd.notna(row['genre']):
            for genre in row['genre'].split(","):
                genreFine = genre.strip()
                movie = row['names']
                if movie in genresInMovies:
                    genresInMovies[movie].append(genreFine)
                else:
                    genresInMovies[movie] = [genreFine]
    return genresInMovies


def differentGenders(df):
    differentGenre = []
    for data in df["genre"]:
        # Check if data is not NaN
        if pd.notna(data):
            for gender in data.split(","):
                differentGenre.append(gender.strip())
    return list(set(differentGenre))


def populateTable(connection, table_name, df):
    i = 1
    indexMovie = dict()
    cursor = connection.cursor()
    for index, row in df.iterrows():
        insert_query = f"""
            INSERT INTO {table_name} (id, names, date_x, score, overview, budget_x, revenue, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            i,
            row['names'],
            pd.to_datetime(row['date_x'], errors='coerce'),
            row['score'],
            row['overview'],
            row['budget_x'],
            row['revenue'],
            row['country']
        )
        cursor.execute(insert_query, values)
        indexMovie[row['names']] = i
        i += 1
        if i % 50 == 0:
            print(f"{i} rows inserted")
    connection.commit()
    print("Data 1 inserted successfully")
    return indexMovie


def populateTable2(connection, table_name, distinctNames):
    cursor = connection.cursor()
    index = 1
    indexActors = dict()

    for name in distinctNames:
        if name == "nan" or name == "None" or name == "" or name == " ":
            continue
        insert_query = f"""
            INSERT INTO {table_name} (id, names)
            VALUES ({index},'{name}')
        """
        cursor.execute(insert_query)
        indexActors[name] = index
        index += 1
        if index % 50 == 0:
            print(f"{index} rows inserted")

    connection.commit()
    print("Data 2 inserted successfully")
    return indexActors


def populateTable3(connection, table_name, distinctGenres):
    cursor = connection.cursor()
    indexGenres = dict()
    index = 1
    for name in distinctGenres:
        if name == "nan" or name == "None" or name == "" or name == " ":
            continue
        insert_query = f"""
            INSERT INTO {table_name} (id, genre)
            VALUES ({index},'{name}')
        """
        cursor.execute(insert_query)
        indexGenres[name] = index
        index += 1
        if index % 50 == 0:
            print(f"{index} rows inserted")
    connection.commit()
    print("Data 3 inserted successfully")
    return indexGenres


def populateTable4(connection, table_name, df, indexMovies: dict, indexActors: dict):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        if pd.notna(row['crew']):
            for actor in row['crew'].split(","):
                actorFine = actor.strip().replace("'", " ").replace('"', " ")
                if row['names'] in indexMovies and actorFine in indexActors:
                    insert_query = f"""
                        INSERT INTO {table_name} (movie_id, actor_id)
                        VALUES ({indexMovies[row['names']]}, {indexActors[actorFine]})
                    """
                    cursor.execute(insert_query)
        if index % 50 == 0:
            print(f"{index} rows inserted")

    connection.commit()
    print("Data 4 inserted successfully")


def populateTable5(connection, table_name, df, indexMovies: dict, indexGenres: dict):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        if pd.notna(row['genre']):
            for genre in row['genre'].split(","):
                genreFine = genre.strip()
                if row['names'] in indexMovies and genreFine in indexGenres:
                    insert_query = f"""
                        INSERT INTO {table_name} (movie_id, genre_id)
                        VALUES ({indexMovies[row['names']]}, {indexGenres[genreFine]})
                    """
                    cursor.execute(insert_query)
        if index % 50 == 0:
            print(f"{index} rows inserted")

    connection.commit()
    print("Data 5 inserted successfully")


def populateTable6(connection, df, indexMovies: dict, indexActors: dict, indexGenres: dict, actorsInMovies: dict, genresInMovies: dict):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        if row["names"] in actorsInMovies and row["names"] in indexMovies:
            idMovie = indexMovies[row["names"]]
            for actor in actorsInMovies[row["names"]]:
                if actor in indexActors:
                    idActor = indexActors[actor]
                    insert_query = f"""
                        INSERT INTO moviesActors (movie_id, actor_id)
                        VALUES ({idMovie}, {idActor})
                    """
                    cursor.execute(insert_query)
            for genre in genresInMovies[row["names"]]:
                if genre in indexGenres:
                    idGenre = indexGenres[genre]
                    insert_query = f"""
                        INSERT INTO moviesGenres (movie_id, genre_id)
                        VALUES ({idMovie}, {idGenre})
                    """
                    cursor.execute(insert_query)
        if (index+1) % 10 == 0:
            print(f"{index+1} rows inserted")

    connection.commit()
    print("Data 4-5 inserted successfully")


def get_n_elements(table_name, connection, n):
    # Create a cursor
    cursor = connection.cursor()

    # Select the first 5 movies from the table
    select_query = f"""
        SELECT * FROM {table_name}
        LIMIT {n}
    """
    cursor.execute(select_query)
    result = cursor.fetchall()
    pprint(result)


def createTables(connection):
    columnsMovies = getColumnsMovies()
    columnsActors = getColumnsActors()
    columnsGenders = getColumnsGenders()
    createTable(connection, "movies", columnsMovies)
    createTable(connection, "actors", columnsActors)
    createTable(connection, "genre", columnsGenders)
    createMany2Many(connection)


def populate(connection):
    df = readData()
    names = differentActors(df)
    genders = differentGenders(df)
    indexMovies = populateTable(connection, "movies", df)
    indexActors = populateTable2(connection, "actors", names)
    indexGenders = populateTable3(connection, "genre", genders)
    actorsInMoviesDict = actorsInMovies(df)
    genresInMoviesDict = genresInMovies(df)
    populateTable6(connection, df, indexMovies,
                   indexActors, indexGenders, actorsInMoviesDict, genresInMoviesDict)

    # populateTable4(connection, "moviesActors", df, indexMovies, indexActors)
    # populateTable5(connection, "moviesGenres", df, indexMovies, indexGenders)


def showProducts(connection, n):
    get_n_elements("movies", connection, n)
    get_n_elements("actors", connection, n)
    get_n_elements("genre", connection, n)
    get_n_elements("moviesActors", connection, n)
    get_n_elements("moviesGenres", connection, n)


def deleteAllTables(connection):
    deleteTable(connection, "moviesActors")
    deleteTable(connection, "moviesGenres")
    deleteTable(connection, "movies")
    deleteTable(connection, "actors")
    deleteTable(connection, "genre")
