import requests
from datetime import datetime
from pprint import pprint
import json
import os
import subprocess
import time


def _get(r):
    headers = {"Accept": "application/json"}
    return requests.get(r, headers=headers)


def _post(r, payload):
    return requests.post(r, json=payload)


def get_yarn_scheduler(host='localhost', port='8088'):
    r = 'http://{0}:{1}/ws/v1/cluster/scheduler'.format(host, port)
    return _get(r)


def get_yarn_app(host='localhost', port='8088', app=None):
    r = 'http://{0}:{1}/ws/v1/cluster/apps/{2}'.format(host, port, app)
    return _get(r)


def get_yarn_logs(applicationId, appOwner):
    logs = subprocess.check_output(
        ['yarn', 'logs', '-applicationId', '{0}'.format(applicationId), '-appOwner', '{0}'.format(appOwner)])

    with open('{0}.log'.format(applicationId), 'w') as log_file:
        for line in logs:
            log_file.write(line)

    return 0


def upload_logs(applicationId, channel="#test", token='50-char-valid-token'):
    curl_command = "curl -F file=@{0}.log -F channels={1} -F token={2} https://slack.com/api/files.upload".format(
        applicationId, channel, token).split()
    try:
        subprocess.call(curl_command)
    finally:
        remove_old_yarn_logs(applicationId)


def post_to_slack(attachment, channel="#test", r="https://hooks.slack.com/services/<9-char>/<9-char>/<24-char>"):
    payload = {"channel": channel,
               "username": "tailor",
               "attachments": attachment,
               "icon_emoji": ":eyeglasses:"
               }
    return _post(r, payload)


def generate_attachment(app):
    if app['finalStatus'] == "SUCCEEDED":
        color = "#008000"
    elif app['finalStatus'] == "RUNNING":
        color = "#0000FF"
    elif app['finalStatus'] == "FAILED":
        color = "#FF0000"
    else:
        color = "#808080"

    m, s = divmod(app['elapsedTime'], 60000)
    h, m = divmod(m, 60)

    totalTime = "%d:%02d:%02d" % (h, m, s / 1000)
    return [{"fallback": "Application {0}. Name: {1}. ".format(app['id'], app['name']),
             "pretext": "Application: {0}".format(app['name']),
             "title": "{0}".format(app['id']),
             "title_link": "{0}".format(app['trackingUrl']),
             "fields": [
                 {
                     "title": "State",
                     "value": "{0}".format(app['finalStatus']),
                     "short": True
                 },
                 {
                     "title": "Run Time",
                     "value": "{0}".format(totalTime),
                     "short": True
                 },
                 {
                     "title": "Start Time",
                     "value": "{0}".format(
                         datetime.fromtimestamp(int(app['startedTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S %Z')),
                     "short": True
                 },
                 {
                     "title": "End Time",
                     "value": "{0}".format(
                         datetime.fromtimestamp(int(app['finishedTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S %Z')),
                     "short": True
                 }
             ],
             "color": color
             }]


def write_yarn_data(filename, data):
    with open(filename, 'w') as outfile:
        return json.dump(data, outfile)


def read_yarn_data(filename):
    with open(filename, 'r') as infile:
        return json.load(infile)


def remove_old_yarn_logs(applicationId):
    try:
        os.remove('{0}.log'.format(applicationId))
    except OSError:
        print "log {0}.log not found".format(applicationId)
        pass


if __name__ == "__main__":
    yarn_master = "192.168.10.84"
    slack_hook = "https://hooks.slack.com/services/<9-char>/<9-char>/<24-char>"
    upload_token = "<50-char>"
    slack_channel = "#test"
    yarn_app_name_filter = ("names", "to", "filter", "on")

    # how long to sleep before checking YARN again
    sleep_time = 60

    while True:
        all_apps = get_yarn_app(host=yarn_master, app="").json()
        print "cleaning apps"
        cleaned_apps = {}
        print all_apps['apps']
        # yarn nests the apps too deeply for my liking
        for app in all_apps['apps']['app']:
            cleaned_apps[app['id']] = app

        print len(all_apps['apps']['app'])
        print len(cleaned_apps.keys())
        # read in any previously fetched apps to check if they've changed
        try:
            prev_apps = read_yarn_data("yarn-apps.json")
        except:
            # assumes fresh run
            write_yarn_data("yarn-apps.json", cleaned_apps)
            prev_apps = cleaned_apps
            print "No yarn-apps.json found. Writing current apps."

        for app in cleaned_apps.keys():
            if cleaned_apps[app]['name'].startswith(yarn_app_name_filter):
                try:
                    # the app has finished, but failed after running or isn't in the previous apps (short running apps < sleep_time)
                    if cleaned_apps[app]['state'] == "FINISHED" and cleaned_apps[app]['finalStatus'] == "FAILED" and (
                            prev_apps[app]['state'] == "RUNNING" or app not in prev_apps):
                        print cleaned_apps[app]
                        print "generating attachment"
                        attachment = generate_attachment(cleaned_apps[app])
                        print "posting to slack"
                        post_to_slack(attachment, slack_hook)

                        try:
                            print "trying to get and upload logs"
                            get_yarn_logs(cleaned_apps[app]['id'])
                            upload_logs(cleaned_apps[app]['id'], slack_channel, upload_token)
                        except:
                            print "getting yarn logs for {0} failed".format(cleaned_apps[app]['id'])


                except KeyError:
                    if app not in prev_apps and cleaned_apps[app]['state'] == "FINISHED" and cleaned_apps[app][
                        'finalStatus'] == "FAILED":
                        print cleaned_apps[app]
                        print "generating attachment"
                        attachment = generate_attachment(cleaned_apps[app])
                        print "posting to slack"
                        post_to_slack(attachment, slack_hook)
                        try:
                            print "trying to get and upload logs"
                            get_yarn_logs(cleaned_apps[app]['id'])
                            upload_logs(cleaned_apps[app]['id'], slack_channel, upload_token)
                        except:
                            print "getting yarn logs for {0} failed".format(cleaned_apps[app]['id'])

        write_yarn_data("yarn-apps.json", cleaned_apps)
        print datetime.now()
        time.sleep(sleep_time)
