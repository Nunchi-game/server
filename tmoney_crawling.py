from selenium import webdriver
import time
import sys
import json
import datetime
import pandas as pd
import os
import logging

#data frame MySQL insert.
import pymysql
#get log
import logging

from sqlalchemy import create_engine

# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
##출발지, 도착지 json file 읽기

def main():
    try:
        engine = create_engine(
            "mysql+mysqldb://root:nunchi@220.67.128.71:3306/nunchi",
            encoding='utf-8')
        conn = engine.connect()

    except:
        logging.error("cannot connect database")
        sys.exit(1)

    print("tbus start!!!!")
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome('./chromedriver', chrome_options=options)

    # dept_list = [{"trml_Cd": "0511601", "trml_Nm": "동서울", "region" : "수도권"}]
    #ariv_list = [{"trml_Cd": "4773401", "trml_Nm": "부산동래", "region": "경남"}]
    with open('tmoneyTerminal/departTerm.json', 'r', encoding='utf-8') as json_file:
        dept_list = json.load(json_file)
    with open('tmoneyTerminal/arrivalTerm.json', 'r', encoding='utf-8') as json_file:
        ariv_list = json.load(json_file)
    ###현재 일자 폴더명 생성
    
    #date = nowTime.strftime('%m%d')
    #directory = os.path.join(os.getcwd(), 'tmoney_data', 'tmoney'+date)
    #print(directory)
    # -------------현재 날짜에 맞는 폴더 생성------------#
    # !!!!!!!!!!!!!!!!예외처리 해야됨 : 폴더명 중복, 파일 생성 오류 등등
    #try:
    #    if not os.path.exists(directory):
    #        os.mkdir(directory)
    #except:
    #    print("Failed to create directory!")
    columnNames = ['arrival', 'total', 'remain', 'reserved', 'city']
    seatState = pd.DataFrame(columns=columnNames)

    ##도착지 터미널 반복문
    try:
        for ariv_term in ariv_list:
            addArivTrmlInfo = 'addTrmlInfo("02", "' + ariv_term['trml_Nm'] + '", "' + ariv_term[
                'trml_Cd'] + '")'  ##도착지 선택 javascript 함수
            # showDeprLayer = "showLayer($('[tmoneyTerminal-layer-name=\'terminal_select01\']'))" #터미널 선택창 열기
            # showArivLayer = "showLayer($('[tmoneyTerminal-layer-name=\'terminal_select02\']'))"
            # 하나의 터미널에 대해서 해당 날의 전제 좌석, 예약 좌석, 잔여좌석 계산
            left = 0
            total = 0
            #print('도착 터미널 : ' + ariv_term['trml_Nm'])
            for dept_term in dept_list:
                driver.get("https://txbus.t-money.co.kr/main.do")
                time.sleep(3)
                addDeptTrmlInfo = 'addTrmlInfo("01", "' + dept_term['trml_Nm'] + '", "' + dept_term[
                    'trml_Cd'] + '")'  ##출발지 선택 javascript 함수
                print('출발 터미널 : ' + dept_term['trml_Nm'])
                driver.execute_script(addDeptTrmlInfo)
                try:  # 경고창이 뜨는 지 확인
                    alert = driver.switch_to_alert()
                    alert.accept()  # 경고창 확인 누르기
                except:
                    "There is not alert"
                # time.sleep(1)
                driver.execute_script(addArivTrmlInfo)
                try:  # 경고창이 뜨는 지 확인
                    alert = driver.switch_to_alert()
                    alert.accept()  # 경고창 확인 누르기
                except:
                    "There is not alert"
                    # 조회하기 클릭
                selectSearch = driver.find_element_by_css_selector("li.menu1 p.btn_search a")
                selectSearch.click()
                time.sleep(2)
                agreeBtn = driver.find_element_by_css_selector("li.menu1 div.btnArea a:nth-of-type(1)")
                agreeBtn.click()
                time.sleep(2)
                seatInfo = driver.find_elements_by_css_selector(
                    "div.accordian_table.pc_ver div.td_wrap1 a.btn_reservation strong")

                for seat in seatInfo:
                    info = seat.text.split('/')
                    left += int(info[0].split('석')[0])
                    total += int(info[1][1:3])
                time.sleep(1)
            ###----출발지 반복문----###

            new_terminal_row = pd.Series([ariv_term['trml_Nm'], total, left, total - left, ariv_term['city']],
                                         index=columnNames)  # [터미널, 전체좌석, 예약좌석, 잔여좌석]
            seatState = seatState.append(new_terminal_row, ignore_index=True)
            print(ariv_term['trml_Nm'] + 'is finished')
            # time.sleep(2)
        ##-----도착지 반복문 ------##
    except Exception as e:
        nowTime = datetime.datetime.now()
        date_time = nowTime.strftime("%m_%d_%Y_")
        logging.basicConfig(filename='./log'+date_time+'_tmoney.log', level=logging.WARNING)
        logging.warning(e)
    finally:
        driver.close()
        driver.quit()
        conn.close()
        engine.dispose()
    #seatState.to_csv(directory + '/' + 'result.csv', encoding='utf-8')

    #dataframe to database
    seatState.to_sql(name="tmoneybus_result", con=engine, if_exists='append', index=False)

    #with open(directory + '/' + 'api_result.json', 'w', encoding='utf-8') as api_result:
    #   seatState.to_json(api_result, orient='table', force_ascii=False)
    conn.close()
    engine.dispose()

if __name__=='__main__':
    main()
