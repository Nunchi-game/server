from selenium import webdriver
import time
#데이터 전처리 위해서
import pandas as pd
#폴더 생성 등을 위해
import os
#날짜 받아오기 위해
import datetime

def main():
  my_path = os.path.abspath(os.path.dirname(__file__))
  path = os.path.join(my_path, "./../carDataZip/")
  #크롬드라이버 옵션

  options = webdriver.ChromeOptions()
  options.add_argument("headless")
  options.add_experimental_option("prefs", {
    "download.default_directory": r"/Users/yb/PycharmProjects/server/carDataZip",
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False, #안전 다운로드 기능 비활성화
    'safebrowsing.disable_download_protection': True
  })

  ######################웹드라이버 접속,다운받기#########################
  chromePath = os.path.join(my_path, "../chromedriver")
  driver = webdriver.Chrome(chromePath, chrome_options=options)
  driver.get("http://data.ex.co.kr/portal/fdwn/view?type=TCS&num=34&requestfrom=dataset#")
  driver.implicitly_wait(5)

  #집계주기 1일로 선택
  driver.find_element_by_xpath("//*[@id='collectCycle']/option[2]").click()

  #조회하기 선택,디운
  search_buttion = driver.find_element_by_css_selector(".fdwnSearchBtnArea")
  search_buttion.click()

  search_buttion = driver.find_element_by_css_selector("#fileDownload")
  search_buttion.click()
  time.sleep(10)

  driver.close

  time.sleep(5)
  ######################다운 받은 파일 열고, 압축 해제하기########################
  #날짜가 어긋날 수 있으므로 오류처리 해 주도록 하자

  #어제날짜 알아오기

  today = datetime.date.today()
  yesterday = today - datetime.timedelta(1)
  date = yesterday.strftime('%m%d')
  filename = "TCS_영업소별교통량_1일_1일_2020"+date #다운이 성공적으로 됐다면, 존재할 파일 이름

  toll_list = pd.read_csv('crawling/toll_list.csv', engine='python')#전처리 위해 tollgate list 가져오기

  os.rename(path+filename+'.zip', path+filename+'.gz')

  df = pd.read_csv(path+filename+'.gz', compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False, encoding='CP949', index_col=0)

  df = df[df['입출구구분코드']==0] # 입구
  df.drop(['TCS하이패스구분코드','고속도로운영기관구분코드','영업형태구분코드','3종교통량','4종교통량','5종교통량','6종교통량','총교통량','Unnamed: 13'],axis=1,inplace=True)
  df = df.groupby(['집계일자','영업소코드']).sum()
  df = df.reset_index()
  df = pd.merge(df,toll_list,left_on = '영업소코드',right_on='station_code')
  df.drop(['영업소코드','입출구구분코드'],axis=1,inplace=True)
  df.rename(columns = {'집계일자':'date','1종교통량' : 'car','2종교통량' : 'bus'}, inplace = True)
  df['date']=df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
  df["station_code"] = df["station_code"].astype(str)
  df.replace({'station_code': '13'}, {'station_code': '013'}, inplace=True)

  df = df[['date', 'station_code', 'station_name', 'car', 'bus', 'city']]
  return df

if __name__ == '__main__':
    main()