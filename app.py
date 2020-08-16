from flask import Flask
import json
import kobus_crawling

from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)


sched = BackgroundScheduler()
sched.start()

# 주기적으로 crawling 하여 저장.
def crawlingKobusEveryDay():
    # crawling file 실행.
    try:
        kobus_crawling.main()
    except Exception as ex:
        print(ex)


sched.add_job(crawlingKobusEveryDay,'cron', minute="0", second="0", hour="0",id="crawlingJob")


"""
    FE으로부터 crawling 요청 받아서 crawling 파일 실행.
"""

"""
    spring으로 부터 요청 받아서 result.json 파일 파싱해서 넘겨주기.
"""
@app.route('/crawling/kobus', methods = ['GET'])
def crawlingKobus():
    try:
        # json file 읽어서 객체에 저장하고
        with open('result.json') as json_file:
            json_data = json.load(json_file)

        return json.dumps({'success': True, 'data': json_data}), 200, {'ContentType': 'application/json'}
    except Exception as ex:
        print(ex)
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}

if __name__ == '__main__':
    app.run()
