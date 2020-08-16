from flask import Flask
from flask import request
import json
import kobus_crawling
import tmoney_crawling

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
sched.add_job(crawlingKobusEveryDay, 'cron', minute="11", second="0", hour="18",id="kobusCrawlingJob")
sched.add_job(crawlingTmoneyBusEveryDay, 'cron', minute="11", second="0", hour="18", id="tmoneybusCrawlingJob")

'''
# bus tmoneyData parsing module.
def getKobusResult():
    try:
        # json file 읽어서 객체에 저장하고
        with open('result.json') as json_file:
            json_data = json.load(json_file)

        return json.dumps({'success': True, 'tmoneyData': json_data}), 200, {'ContentType': 'application/json'}
    except Exception as ex:
        print(ex)
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}

# bus tmoneyData parsing module.
def getTmoneybusResult():
    try:
        # json file 읽어서 객체에 저장하고
        print(1)
    except Exception as ex:
        print(ex)



# bus tmoneyData 불러오기.
@app.route('/api/bus', methods = ['GET'])
def getBusData():

    data = request.data;

# car tmoneyData 불러오기.
@app.route('/api/car', methods = ['GET'])
def getCarData():



if __name__ == '__main__':
    app.run()
'''