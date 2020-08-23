from flask import Flask
from flask import request
import json
import kobus_crawling
import tmoney_crawling
import pymysql
import logging
import sys
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

sched = BackgroundScheduler()
sched.start()

# 주기적으로 crawling 하여 저장.
# kobus crawling module.
def crawlingKobusEveryDay():
    # crawling file 실행.
    try:
        kobus_crawling.main()
    except Exception as ex:
        print(ex)

# tmoney bus crawling module.
def crawlingTmoneyBusEveryDay():
    # crawling file 실행.
    try:
        tmoney_crawling.main()
    except Exception as ex:
        print(ex)


# 매일 자정 크롤링
sched.add_job(crawlingKobusEveryDay, 'cron', minute="34", second="0", hour="20",id="kobusCrawlingJob")
sched.add_job(crawlingTmoneyBusEveryDay, 'cron', minute="34", second="0", hour="20", id="tmoneybusCrawlingJob")


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
        sql = "SELECT id, arrival, total, remain, reserved, region FROM nunchi.kobus_result WHERE created_at LIKE %s"
        cursor.execute(sql, nowDate)
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
        sql = "SELECT id, arrival, total, remain, reserved, region FROM nunchi.tmoneybus_result WHERE created_at LIKE %s"
        cursor.execute(sql, nowDate)
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

def groupByRegion(database):
    try:
       # database = "nunchi."+database
        conn = pymysql.connect(host="220.67.128.71", user="root", passwd="nunchi", db="nunchi", port=3306,
                               use_unicode=True,
                               charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    nowDate = datetime.today().strftime("%Y-%m-%d")
    nowDate += "%"
    sql = "SELECT SUM(reserved) AS reserved , region FROM %s WHERE created_at LIKE %s GROUP BY region"
    cursor.execute(sql, (database, nowDate))
    row_headers = [x[0] for x in cursor.description]
    rv = cursor.fetchall()
    json_data = []
    for result in rv:
        json_data.append(dict(zip(row_headers, result)))

    conn.commit()
    cursor.close()
    return json.dumps(json_data)

# bus tmoneyData 불러오기.
@app.route('/api/bus', methods = ['GET'])
def getBusData():
    kobusData = groupByRegion('kobus_result')
    tmoneybusData = groupByRegion('tmoneybus_result')
    print(kobusData)
    print(tmoneybusData)

'''
# car tmoneyData 불러오기.
@app.route('/api/car', methods = ['GET'])
def getCarData():
'''


if __name__ == '__main__':
    app.run()
