import decimal
import csv
from flask import Flask
from flask import request
import requests
import json
import kobus_crawling
import tmoney_crawling
import pymysql
import logging
import sys
from datetime import datetime
import interchangeList
import pandas as pd
from decimal import Decimal
from sqlalchemy import create_engine, types

from apscheduler.schedulers.background import BackgroundScheduler

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


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

start_hour = "11"
start_minute = "44"
start_second = "0"
# 매일 자정 크롤링
sched.add_job(crawlingKobusEveryDay, 'cron', minute="23", second="0", hour="0",id="kobusCrawlingJob")
sched.add_job(crawlingTmoneyBusEveryDay, 'cron', minute="23", second="0", hour="0", id="tmoneybusCrawlingJob")


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

# post data to database by city.
def dataframeToDatabase():
    try:
        engine = create_engine(
            "mysql+mysqldb://root:nunchi@220.67.128.71:3306/nunchi",
            encoding='utf-8')
        conn = engine.connect()

    except:
        logging.error("cannot connect database")
        sys.exit(1)

    try:
        result = pd.read_csv('final_total.csv', sep=',', encoding='utf8')

        isSeoul = result['city'] == '서울'
        seoul = result[isSeoul]
        isYangpyeong = result['city'] == '양평'
        yangpyeong = result[isYangpyeong]
        isDaegu = result['city'] == '대구'
        daegu = result[isDaegu]
        isBusan = result['city'] == '부산'
        busan = result[isBusan]
        isChangwon = result['city'] == '창원'
        changwon = result[isChangwon]
        isTongyeong = result['city'] == '통영'
        tongyeong = result[isTongyeong]
        isHoengseong = result['city'] == '횡성'
        hoengseong = result[isHoengseong]
        isHongcheon = result['city'] == '홍천'
        hongcheon = result[isHongcheon]
        isPyeongchang = result['city'] == '평창'
        pyeongchang = result[isPyeongchang]
        isGangneung = result['city'] == '강릉'
        gangneung = result[isGangneung]
        isYangyang = result['city'] == '양양'
        yangyang = result[isYangyang]
        isSokcho = result['city'] == '속초'
        sokcho = result[isSokcho]
        isChuncheon = result['city'] == '춘천'
        chuncheon = result[isChuncheon]
        isGwangyang = result['city'] == '광양'
        gwangyang = result[isGwangyang]
        isGwangju = result['city'] == '광주'
        gwangju = result[isGwangju]
        isSuncheon = result['city'] == '순천'
        suncheon = result[isSuncheon]
        isMokpo = result['city'] == '목포'
        mokpo = result[isMokpo]
        isGyeongju = result['city'] == '경주'
        gyeongju = result[isGyeongju]
        isJeonju = result['city'] == '전주'
        jeonju = result[isJeonju]
        isGunsan = result['city'] == '군산'
        gunsan = result[isGunsan]

        dataframeList = [seoul, yangpyeong, daegu, busan, changwon, tongyeong, hoengseong, hongcheon, pyeongchang,
                       gangneung, yangyang, sokcho, chuncheon, gwangyang, gwangju, suncheon, mokpo, gyeongju,
                       jeonju, gunsan]
        i = 0
        for dataframe in dataframeList:
            dataframe.to_sql(name = interchangeList.arrivalList[i], con=engine, index=False, if_exists='append')
            i += 1

        #result.to_sql(name='car_result',con=engine,index=False,if_exists='append')
        conn.close()
    except Exception as e:
        conn.close()
        print(e)

# get training set from database.
def getTrainingSet():
    try:
        engine = create_engine(
            "mysql+mysqldb://root:nunchi@220.67.128.71:3306/nunchi",
            encoding='utf-8')
        conn = engine.connect()

    except:
        logging.error("cannot connect database")
        sys.exit(1)

    try:
        dataFrameList = []
        for i in range(len(interchangeList.arrivalList)):
            query = "SELECT * FROM {}".format(interchangeList.arrivalList[i])
            result = conn.execute(query)
            rows = result.fetchall()
            globals()[interchangeList.arrivalList[i]] = pd.DataFrame(rows,columns=['data', 'station_code', 'station_name', 'car' ,'bus', 'city'])
            dataFrameList.append(globals()[interchangeList.arrivalList[i]])
        conn.close()

        return dataFrameList
    except Exception as e:
        conn.close()
        print(e)

@app.route('/api/car', methods=['GET'])
def getCarData():

    a = getTrainingSet()
    print(a)

    return {'StatusCode': '200', 'Message': 'Get bus result success'}



# bus tmoneyData 불러오기.
@app.route('/api/bus', methods = ['GET'])
def getBusData():
    try:
        kobusData = groupByRegion('kobus_result')
        tmoneybusData = groupByRegion('tmoneybus_result')
        busResult = {}
        for num, data in kobusData:
            if(data in busResult.keys()):
                # 이미 있는 값이면.
                busResult[data] += num
            else:
                busResult[data] = num


        for num, data in tmoneybusData:
            if (data in busResult.keys()):
                # 이미 있는 값이면.
                busResult[data] += num
            else:
                busResult[data] = num
        print(busResult)

        busResult = json.dumps(busResult, cls=DecimalEncoder, ensure_ascii=False)

        return {'StatusCode': '200', 'Message': 'Get bus result success', 'data': json.loads(busResult)}
    except Exception as e:
        print(e)
        return {'StatusCode': '400', 'Message': 'Get bus result fail'}

'''
# car tmoneyData 불러오기.
@app.route('/api/car', methods = ['GET'])
def getCarData():
'''


if __name__ == '__main__':
    app.run()
