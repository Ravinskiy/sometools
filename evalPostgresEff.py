# -*- coding: utf-8 -*-
import datetime
import json
from statistics import mean, median, stdev
import psycopg2

ITERATIONS = 1000


def now_timestamp():
    return datetime.datetime.now().timestamp()


def test_query(connection, cursor, extId, type, iterations):
    conn = connection
    cur = cursor
    cur.execute("UPDATE item_counters SET (value_int) = (0) \
        WHERE ext_id = %s AND type = %s;" % (extId, type))
    conn.commit()
    statsDict = dict()
    statsList = list()
    for i in range(1, iterations):
        testStart = now_timestamp()
        updatedAt = now_timestamp()
        cur.execute(
            "UPDATE item_counters \
            SET (value_int, updated_at) = (value_int+1, %s) \
            WHERE ext_id = %s AND type = %s;" % (updatedAt, extId, type)
        )
        conn.commit()
        testEnd = now_timestamp()
        execTime = testEnd - testStart
        statsList.append(execTime)

    cur.execute(
        "UPDATE item_counters SET (value_int) = (0) \
        WHERE ext_id = %s AND type = %s;" % (extId, type)
    )
    conn.commit()

    statsDict['iterations'] = iterations
    statsDict['mean'] = mean(statsList)
    statsDict['stdev'] = stdev(statsList)
    statsDict['median'] = median(statsList)
    return statsDict


if __name__ == "__main__":
    statsDict = dict()

    conn = psycopg2.connect(
        database='database',
        user='postgres',
        password='password',
        host='8.8.8.8',
        port=5432
    )
    cur = conn.cursor()

    try:
        cur.execute('DROP INDEX item_counters_type_value_int_asc;')
        conn.commit()
        print('Index dropped\n')
    except psycopg2.ProgrammingError:
        print('Index not found\n')
        conn.commit()

    statsDict['withoutIndexQuery1'] = test_query(
        conn,
        cur,
        4601557000169,
        8,
        ITERATIONS
    )
    statsDict['withoutIndexQuery2'] = test_query(
        conn, cur, 101387, 3, ITERATIONS
    )

    indexingStart = now_timestamp()
    cur.execute(
        'CREATE INDEX item_counters_type_value_int_asc \
        ON item_counters USING btree (type, value_int);'
    )
    conn.commit()
    indexingEnd = now_timestamp()
    indexingTime = indexingEnd - indexingStart
    print('Index created\n')

    statsDict['indexing'] = {'time': indexingTime}
    statsDict['withIndexQuery1'] = test_query(
        conn,
        cur,
        4601557000169,
        8,
        ITERATIONS
    )
    statsDict['withIndexQuery2'] = test_query(conn, cur, 101387, 3, ITERATIONS)

    cur.close()
    conn.close()

    statsJson = json.dumps(statsDict, sort_keys=True, indent=4)
    f = open('queryStats.json', 'w')
    f.write(statsJson)
    f.close()
    print('Stats loaded to JSON file\n')
