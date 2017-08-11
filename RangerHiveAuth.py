# coding=utf-8
# !/usr/bin/python

from requests.auth import HTTPBasicAuth
import requests
import sys
import json


def get_repository(repository_id):
    response = requests.get(range_api_url + 'service/public/api/repository/' + repository_id,
                            auth=(auth_name, auth_pass))
    print json.loads(response.text)


def create_repository(name, description, username, password, jdbc_url):
    config_data = {'username': username, 'password': password,
                   'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver', 'jdbc.url': jdbc_url,
                   'commonNameForCertificate': ''}
    json_data = {'name': name, 'description': description, 'repositoryType': 'Hive', 'config': json.dumps(config_data),
                 'isActive': True}

    response = requests.post(range_api_url + 'service/public/api/repository', data=json.dumps(json_data))
    print json.loads(response.text)


def update_repository(repository_id, name, description, username, password, jdbc_url):
    config_data = {'username': username, 'password': password,
                   'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver', 'jdbc.url': jdbc_url,
                   'commonNameForCertificate': ''}
    json_data = {'name': name, 'description': description, 'repositoryType': 'Hive', 'config': json.dumps(config_data),
                 'isActive': True}

    response = requests.put(range_api_url + 'service/public/api/repository' + repository_id, data=json.dumps(json_data))
    print json.loads(response.text)


def delete_repository(repository_id):
    response = requests.delete(range_api_url + 'service/public/api/repository/' + repository_id)
    print json.loads(response.text)


def search_repository(pagesize, startindex, name, status):
    payload = {'pageSize': pagesize, 'startIndex': startindex, 'name': name, 'type': 'hive', 'status': status}
    response = requests.get(range_api_url + 'service/public/api/repository', params=payload)
    print json.loads(response.text)


def get_policy(policy_id):
    response = requests.get(range_api_url + 'service/public/api/policy/' + policy_id)
    print json.loads(response.text)


def delete_policy(policy_id):
    response = requests.delete(range_api_url + 'serveice/public/api/policy/' + policy_id)
    print json.loads(response.text)


def create_policy(param_json):
    response = requests.post(range_api_url + 'service/public/api/policy/', json=param_json)
    print json.loads(response.text)


if __name__ == '__main__':
    if len(sys.argv) == 6:
        filename = sys.argv[1]
        range_api_url = sys.argv[2]
        auth_name = sys.argv[3]
        auth_pass = sys.argv[4]
        operation_name = sys.argv[5]
    else:
        print " Usage:  ./RangerHiveAuth.py <filename> <ranger_api_url> <auth_name> <auth_pass> <operation_name>"
        quit(1)
    if operation_name == 'get_repository':
        get_policy()


# with open(filename, "r") as ins:
#     for line in ins:
#         str_split = line.split('|')
#         print str_split
