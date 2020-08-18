from selenium import webdriver
import time
import sys
#데이터 전처리 위해서
import pandas as pd
#폴더 생성 등을 위해
import os 
#날짜 받아오기 위해
import datetime
#json 파일 형식으로 df 저장하기 위해서
import json
#data frame MySQL insert.
import pymysql
#get log
import logging

from sqlalchemy import create_engine

# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
import MySQLdb


class DepartureTime(object):
    def __init__(self,departureTime):
        self.departureTime = departureTime
    def __str__(self):
        return self.departureTime
    def __repr__(self):
        return self.departureTime

def main():
    try:
        engine = create_engine(
            "mysql+mysqldb://root:nunchi@220.67.128.71:3306/nunchi",
            encoding='utf-8')
        conn = engine.connect()

    except:
        logging.error("cannot connect database")
        sys.exit(1)


    #print("kobus start!!!")
    ###출발지,도착지,region 불러오기
    with open('./kobusTerminal/departures.json', 'r', encoding='utf-8') as json_file:
        departures = json.load(json_file)

    with open('./kobusTerminal/arrivals.json', 'r', encoding='utf-8') as json_file:
        arrivals = json.load(json_file)
            
    with open('./kobusTerminal/regions.json', 'r', encoding='utf-8') as json_file:
        regions = json.load(json_file)

    ###현재 일자 폴더명 생성(%H지움)
    nowTime = datetime.datetime.now()
    date = nowTime.strftime('%m%d')
    fileName = 'kobus'+date
    #-------------현재 날자에 맞는 폴더 kobus_data 폴더에 생성,------------#
    #!!!!!!!!!!!!!!!!예외처리 해야됨 : 폴더명 중복, 파일 생성 오류 등등
    try:
        if not os.path.exists('./kobus_data/'+fileName):
            os.mkdir('./kobus_data/'+fileName)
    except:
        print("Failed to create directory!")

    #------------------result.json용 데이터프레임 생성----------------#
    resultTable = pd.DataFrame(columns=['arrival','total','remain','reserved','region'])

    # webdirver옵션에서 headless기능을 사용하겠다 라는 내용
    webdriver_options = webdriver.ChromeOptions()
    webdriver_options.add_argument('headless')
    webdriver_options.add_argument('--no-sandbox')


    #---------------도착지 반복문 시작------------#

    for arrival in arrivals:
        arvlCode = arrivals[arrival]
        fnArvlChc = 'fnArvlChc("'+arvlCode+'","'+ arrival +'" ,"' '" ,"' '","' '", "00")'
        ###크롤링 데이터 넣을 데이터프레임 생성
        timeTable = pd.DataFrame(columns=['time','departure','arrival','remain_seat','total_seat','company','grade'])

        #---------------출발지 반복문 시작------------#
        for key, value in departures.items():
            print("kobus new departure start!")
            ###크롬 드라이버 실행,페이지 불러옴
            driver = webdriver.Chrome("./chromedriver", options=webdriver_options)
            driver.get("https://www.kobus.co.kr/oprninf/alcninqr/oprnAlcnPage.do")
            time.sleep(1)

            #코드 실행해서 출발지, 목적지 선택
            fnDeprChc = 'fnDeprChc("'+ value +'","'+ key +'")'
            driver.execute_script(fnDeprChc)
            driver.execute_script(fnArvlChc)

            #조회하기 선택.
            selectSearch = driver.find_element_by_css_selector("p.check")
            selectSearch.click()
            time.sleep(2)

            # 전체 배차표에서 출발시각, 고속사, 등급, 잔여석, 총좌석 크롤링
            stage = 1
            while True:
                try:
                    # 출발시각    
                    getStartTime = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.start_time")
                    # 고속사
                    getCompany = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.bus_com")
                    # 등급
                    getGrade = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.grade")
                    # 잔여석(전처리 해야 함)
                    getRemainSeats = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.remain")
                    # 총좌석
                    getTotalSeats = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.remain span.total_seat")
                except:
                    driver.close()
                    break
                # 잔여석 전처리.
                modifyRemainSeats = getRemainSeats.text
                for i in range(len(modifyRemainSeats)):
                    if modifyRemainSeats[i]==' ':
                        index = i
                        break
                modifyRemainSeats = modifyRemainSeats[:index]
                # 총좌석 전처리.
                modifyTotalSeats = getTotalSeats.text
                for i in range(len(modifyTotalSeats)):
                    if modifyTotalSeats[i]==' ':
                        index = i+1
                        break
                modifyTotalSeats = modifyTotalSeats[index:]
                ##########여기까지 한 row 완성#############

                #데이터프레임에 넣는 과정
                new_row = pd.Series([DepartureTime(getStartTime.text).__str__(),key,arrival,modifyRemainSeats,modifyTotalSeats,getCompany.text,getGrade.text],index=['time','departure','arrival','remain_seat','total_seat','company','grade'])
                timeTable = timeTable.append(new_row,ignore_index=True)
                stage+=1
        #---------------출발지 반복문 종료------------#
        #---------------문자형 column numeric으로 변경------------#
        timeTable['total_seat']=pd.to_numeric(timeTable['total_seat'])
        timeTable['remain_seat']=pd.to_numeric(timeTable['remain_seat'])
        #csvName = arrival+date+'.csv'
        #timeTable.to_csv('./kobus_data/'+fileName+'/'+csvName,encoding='utf-8')
        #---------------json파일 저장을 위한 전처리, json------------#
        result_total = timeTable['total_seat'].sum()
        result_remain = timeTable['remain_seat'].sum()
        result_reserved = result_total - result_remain
        result_region = regions[arrival]
        new_result = pd.Series([arrival,result_total,result_remain,result_reserved,result_region],index=['arrival','total','remain','reserved','region'])
        resultTable = resultTable.append(new_result,ignore_index=True)
        #출발지 반복문 하나씩 돌 때마다 resultTable에 row 하나씩 추가해야 한다.근데 이거 이렇게 해야되나?

    # resultTable dataframe to database
    resultTable.to_sql(name="kobus_result", con=engine, if_exists='append', index=False)
    #이중 반복문 탈출한 뒤 result.json 저장하기
#    resultName = 'result'+date+'.json'
#    with open('./kobus_data/'+fileName+'/'+resultName, 'w', encoding='utf-8') as file:
#        resultTable.to_json(file, orient='table', force_ascii=False)
    conn.close()
    engine.dispose()
if __name__=='__main__':
    main()
