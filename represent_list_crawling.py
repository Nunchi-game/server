# 관광공사 data crawling 하여 언택트 관광지 100선 DB insert

from selenium import webdriver
import sys
import pandas as pd
#get log
import logging
import pymysql
from sqlalchemy import create_engine

# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

webdriver_options = webdriver.ChromeOptions()
webdriver_options.add_argument('headless')
webdriver_options.add_argument('--no-sandbox')

driver = webdriver.Chrome("./chromedriver", options=webdriver_options)
driver.implicitly_wait(10) # seconds
driver.get("https://korean.visitkorea.or.kr/detail/rem_detail.do?cotid=351f6cf4-d984-496f-886d-6fc538b226a7")

try:
    engine = create_engine(
        "mysql+mysqldb://root:nunchi@220.67.128.71:3306/nunchi",
        encoding='utf-8')
    conn = engine.connect()

except:
    logging.error("cannot connect database")
    sys.exit(1)

timeTable = pd.DataFrame(columns=['location', 'link', 'name'])

for i in range(1,101):
    selectSearch = driver.find_element_by_css_selector("ul.con_body_ul a:nth-of-type("+str(i)+")")
    link = selectSearch.get_attribute("href")
    selectSearch = driver.find_element_by_css_selector("ul.con_body_ul a:nth-of-type("+str(i)+") li div.con_title_spot_area p.con_title_spot")
    location = selectSearch.text
    selectSearch = driver.find_element_by_css_selector("ul.con_body_ul a:nth-of-type("+str(i)+") li div.con_title_spot_area p.con_title")
    name = selectSearch.text
    new_row = pd.Series([location, link, name], index=['location', 'link', 'name'])
    timeTable = timeTable.append(new_row, ignore_index=True)

timeTable.to_sql("travel_list", con=engine, if_exists='append', index=False )



