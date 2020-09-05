import logging
import sys
import interchangeList
import pandas as pd
from sqlalchemy import create_engine

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

# get training set from database. for machine learning.
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
            globals()[interchangeList.arrivalList[i]] = pd.DataFrame(rows, columns=['data', 'station_code', 'station_name', 'car' ,'bus', 'city'])
            dataFrameList.append(globals()[interchangeList.arrivalList[i]])
        conn.close()

        return dataFrameList
    except Exception as e:
        conn.close()
        print(e)