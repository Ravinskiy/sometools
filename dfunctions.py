# -*- coding: utf-8 -*-


def get_mongo_client(mongoServer='127.0.0.1', mongoPort=27017):
    from pymongo import MongoClient
    mongoClient = MongoClient(mongoServer, mongoPort)
    return mongoClient


def load_to_mongo(mongoClient, dbName, collName, docsColl):
    if len(docsColl) < 1:
        return False
    try:
        mongoDB = mongoClient[dbName]
        mongoColl = mongoDB[collName]
        result = mongoColl.insert_many(docsColl)
        if len(result.inserted_ids) > 0:
            result = True
        else:
            result = False
    except:
        result = False
    return result


def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and
            # current_row are one character longer
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]
