# -*- coding: utf-8 -*-
import datetime
import base64
import pyodbc
# from pymongo import MongoClient

NOW = datetime.datetime.now()


def log_this(
        mongoClient,
        status,
        message,
        logSource="SomeEventsSource",
        dbName="Logs",
        collName="MyApp"
):
    '''
    Пишет в монгу сообщение с таймстампом (логгирует)
    '''
    try:
        timestamp = datetime.datetime.now()
        mongoDB = mongoClient[dbName]
        mongoColl = mongoDB[collName]
        doc = {
            "timestamp": timestamp,
            "status": status,
            "message": message,
            "source": logSource
        }
        mongoColl.insert(doc)
        result = 0
    except:
        result = 1
    return result


def format_date(dateTime):
    '''
    Форматирует дату из datetime.datetime для XML
    '''
    formatedDate = dateTime.strftime("%Y-%m-%d")
    if formatedDate:
        return formatedDate
    else:
        return None


def format_time(dateTime):
    '''
    Форматирует время из datetime.datetime для XML
    '''
    formatedTime = dateTime.strftime("%H-%M-%S")
    if formatedTime:
        return formatedTime
    else:
        return None


def string_to_base64(s):
    return base64.b64encode(s.encode('utf-8'))


def base64_to_string(b):
    return base64.b64decode(b).decode('utf-8')


def get_MSSQL_connection(serverAddress, dbName, user, passw):
    '''
    Соединяется с сервером MS SQL и возвращает объект соединенния
    '''
    driverString = "{SQL Server Native Client 11.0}"
    connectionString = ("Driver=%s;Server=%s;Database=%s;Uid=%s;Pwd=%s;" %
                        (driverString, serverAddress, dbName, user, passw))
    sqlConnection = pyodbc.connect(connectionString)
    if not sqlConnection:
        sqlConnection = None
    return sqlConnection


def get_MSSQL_cursor(sqlConnection, query):
    '''
    Выполняет запрос и возвращает курсор для итерации
    '''
    cursor = sqlConnection.cursor()
    cursor.execute(query)
    if not cursor:
        cursor = None
    return cursor


def execute_MSSQL_query(sqlConnection, query):
    '''
    Выполняет запрос запрос на запись/апдейт к БД
    '''
    cursor = sqlConnection.cursor()
    cursor.execute(query)
    cursor.commit()
    result = 0
    if not cursor:
        result = 1
    return result
