from urllib import request
import json

import subprocess, sys

ip_address = subprocess.check_output('hostname -i', shell=True).decode('utf-8').strip()
api_url = "http://" + ip_address + ":4040/api/v1/applications/"

WAIT_PERCENTAGE = 50.0

def get_json_from_url(url):
  try: return json.loads(request.urlopen(url).read().decode('utf-8'))
  except Exception as e:
    print('Could not find the object. Make sure you spark session is running and Try Again !')
    sys.exit(1)

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


