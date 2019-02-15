from urllib import request
import json

WAIT_PERCENTAGE = 50.0

api_url = "http://128.104.223.139:4040/api/v1/applications/"
def get_json_from_url(url):
  return json.loads(request.urlopen(url).read())
apps = get_json_from_url(api_url)
app = apps[0]
app_id = app['id']
import time
while True:
    job = get_json_from_url(api_url + app_id + "/jobs/0")
    total_tasks = job['numTasks']
    completed_tasks = job['numCompletedTasks']
    print(f"Progress: {completed_tasks}/{total_tasks}. Wait till {WAIT_PERCENTAGE}%")
    if float(completed_tasks)/total_tasks > WAIT_PERCENTAGE/100:
      print(f"Wait complete as {completed_tasks*100.0/total_tasks} > {WAIT_PERCENTAGE}")
      break
    time.sleep(5)


