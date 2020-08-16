from selenium import webdriver
import time
import sys
import pandas as pd
import os
import datetime
import json


class DepartureTime(object):
    def __init__(self,departureTime):
        self.departureTime = departureTime
    def __str__(self):
        return self.departureTime
    def __repr__(self):
        return self.departureTime

def main():

    departures = {'서울경부':'010','동서울':'032','센트럴시티(서울)':'020','수원':'110','고양백석':'116','용인':'150','성남(분당)':'120','평택':'180','부천':'101',
    '부산':'700','서부산(사상)':'703','동대구':'801','서대구':'805','광주(유·스퀘어)':'500','대전복합':'300','인천':'100','전주':'602','천안':'310','원주':'240','춘천':'250'} 

    arivals = {'서울경부':'010','동서울':'032','센트럴시티(서울)':'020','인천':'100','동대구':'801','서대구':'805','부산':'700','서부산(사상)':'703','마산':'705','마산내서':'706','창원':'710','통영':'730',
    '횡성(휴)하행':'238','강릉':'200','속초':'230','춘천':'250','광양':'520','동광양':'525','광주(유·스퀘어)':'500','순천':'515','여수':'510','목포':'505','포항':'830','경주':'815','전주':'602','군산':'610'} 

    nowTime = datetime.datetime.now()
    date = nowTime.strftime('%m%d%H') 
    fileName = 'kobus'+date
    try:
        os.mkdir('./kobus_data/'+fileName)
    except:
        print("Failed to create directory!")

    for arival in arivals:
        arvlCode = arivals[arival]
        fnArvlChc = 'fnArvlChc("'+arvlCode+'","'+ arival +'" ,"' '" ,"' '","' '", "00")'
        timeTable = pd.DataFrame(columns=['time','departure','arival','remain_seat','total_seat','company','grade'])

        for key, value in departures.items():
            driver = webdriver.Chrome("./chromedriver")
            driver.get("https://www.kobus.co.kr/oprninf/alcninqr/oprnAlcnPage.do")
            time.sleep(1)

            fnDeprChc = 'fnDeprChc("'+ value +'","'+ key +'")'
            driver.execute_script(fnDeprChc)
            driver.execute_script(fnArvlChc)

            selectSearch = driver.find_element_by_css_selector("p.check")
            selectSearch.click()
            time.sleep(2)

            stage = 1
            while True:
                try:
                    getStartTime = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.start_time")
                    getCompany = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.bus_com")
                    getGrade = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.grade")
                    getRemainSeats = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.remain")
                    getTotalSeats = driver.find_element_by_css_selector("div.bus_time p:nth-of-type("+str(stage)+") span.remain span.total_seat")
                except:
                    driver.close()
                    break
                modifyRemainSeats = getRemainSeats.text
                for i in range(len(modifyRemainSeats)):
                    if modifyRemainSeats[i]==' ':
                        index = i
                        break
                modifyRemainSeats = modifyRemainSeats[:index]
                modifyTotalSeats = getTotalSeats.text
                for i in range(len(modifyTotalSeats)):
                    if modifyTotalSeats[i]==' ':
                        index = i+1
                        break
                modifyTotalSeats = modifyTotalSeats[index:]

                new_row = pd.Series([DepartureTime(getStartTime.text).__str__(),key,arival,modifyRemainSeats,modifyTotalSeats,getCompany.text,getGrade.text],index=['time','departure','arival','remain_seat','total_seat','company','grade'])
                timeTable = timeTable.append(new_row,ignore_index=True)
                stage+=1
        csvName = arival+date+'.csv'
        jsonName = arival+date+'.json'
        timeTable.to_csv('./kobus_data/'+fileName+'/'+csvName, encoding='utf-8')
        #json 파일로 만들어서 저장.
        timeTable.to_json('./'+ jsonName, orient="index")



if __name__=='__main__':
    main()
