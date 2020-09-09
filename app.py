import decimal
from flask import Flask
import json
from crawling import kobus_crawling, tmoney_crawling, tollgate_for_day
import controller.bus as busController
import controller.car as carController

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

# 매일 자정 크롤링
sched.add_job(crawlingKobusEveryDay, 'cron', minute="0", second="0", hour="0",id="kobusCrawlingJob")
sched.add_job(crawlingTmoneyBusEveryDay, 'cron', minute="0", second="0", hour="0", id="tmoneybusCrawlingJob")

#df = tollgate_for_day.main()
#carController.dataframeToDatabase(df)

@app.route('/api/represent', methods=['POST'])
def getRepresent():
    '''
    갈만한 여행지 지역 이름 추천받아서 해당 지역내의 관광지 리스트 추천.
    '''
    return { 'StatusCode': '200', 'Message': 'Get bus result success'}

@app.route('/api/car', methods=['GET'])
def getCarData():

    '''
        그 전에 machine learning code 마지막에 database코드 필요.
        get forecast information from database
    '''

    return {'StatusCode': '200', 'Message': 'Get bus result success'}

# bus tmoneyData 불러오기.
@app.route('/api/bus', methods = ['GET'])
def getBusData():
    try:
        kobusData = busController.groupByRegion('kobus_result')
        tmoneybusData = busController.groupByRegion('tmoneybus_result')
        busResult = {}
        for num, data in kobusData:
            if(data in busResult.keys()):
                # 이미 있는 값이면.
                busResult[data] += num
            else:
                busResult[data] = num


        for num, data in tmoneybusData:
            if data in busResult.keys():
                # 이미 있는 값이면.
                busResult[data] += num
            else:
                busResult[data] = num

        busResult = json.dumps(busResult, cls=DecimalEncoder, ensure_ascii=False)

        return {'StatusCode': '200', 'Message': 'Get bus result success', 'data': json.loads(busResult)}
    except Exception as e:
        print(e)
        return {'StatusCode': '400', 'Message': 'Get bus result fail'}


if __name__ == '__main__':
    app.run()
