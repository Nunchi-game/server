import json
import pymysql
import logging
import sys
from datetime import datetime

# bus tmoneyData parsing module.
def getKobusResult():
    try:
        try:
            conn = pymysql.connect(host="220.67.128.71", user="root", passwd="nunchi", db="nunchi", port=3306, use_unicode=True,
                                   charset='utf8')
            cursor = conn.cursor()
        except:
            logging.error("could not connect to rds")
            sys.exit(1)

        # return now date
        nowDate = datetime.today().strftime("%Y-%m-%d")
        nowDate += "%"
        #sql = "SELECT id, arrival, total, remain, reserved, region FROM nunchi.kobus_result WHERE created_at LIKE %s"
        #cursor.execute(sql, nowDate)
        row_headers = [x[0] for x in cursor.description]
        rv = cursor.fetchall()
        json_data = []
        for result in rv:
            json_data.append(dict(zip(row_headers, result)))

        conn.commit()
        cursor.close()
        return json.dumps(json_data)
    except Exception as ex:
        print(ex)
        return 1

# bus tmoneyData parsing module.
def getTmoneybusResult():
    try:
        try:
            conn = pymysql.connect(host="220.67.128.71", user="root", passwd="nunchi", db="nunchi", port=3306,
                                   use_unicode=True,
                                   charset='utf8')
            cursor = conn.cursor()
        except:
            logging.error("could not connect to rds")
            sys.exit(1)

        # return now date
        nowDate = datetime.today().strftime("%Y-%m-%d")
        nowDate += "%"
        #sql = "SELECT id, arrival, total, remain, reserved, region FROM nunchi.tmoneybus_result WHERE created_at LIKE %s"
        #cursor.execute(sql, nowDate)
        row_headers = [x[0] for x in cursor.description]
        rv = cursor.fetchall()
        json_data = []
        for result in rv:
            json_data.append(dict(zip(row_headers, result)))

        conn.commit()
        cursor.close()
        return json.dumps(json_data)
    except Exception as ex:
        print(ex)
        return 1

# get tmoney bus, kobus data by city.
def groupByRegion(database):
    try:
        conn = pymysql.connect(host="220.67.128.71", user="root", passwd="nunchi", db="nunchi", port=3306,
                               use_unicode=True,
                               charset='utf8')
        cursor = conn.cursor()
        cursor.execute("USE nunchi")
    except:
        logging.error("could not connect to database")
        sys.exit(1)

    nowDate = datetime.today().strftime("%Y-%m-%d")
    nowDate = "2020-09-02"
    nowDate += "%"
    try:
        query = "SELECT SUM(reserved) AS reserved , city FROM %s WHERE created_at LIKE %%s GROUP BY city" % database
        cursor.execute(query, nowDate)
        # query = "SELECT total, city FROM kobus_result"
        # cursor.execute(query)
        data = cursor.fetchall()
        data = list(data)
        conn.commit()
        cursor.close()
        return data
    except Exception as e:
        conn.commit()
        cursor.close()
        print(e)