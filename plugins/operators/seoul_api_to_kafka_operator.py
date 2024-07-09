from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

import configparser

config = configparser.ConfigParser()
config.read('../../resources/config.ini')


def _get_area_code():
    area_code = config['API']['AreaCode'].split(',')

    return area_code


def _call_api():
    import requests
    import json
    headers = {'Content-Type': 'application/json',
               'charset': 'utf-8',
               'Accept': '*/*'}

    area_code = _get_area_code()
    api_key = config['API']['ApiKey']

    for code in ['POI003']:
        request_url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/citydata_ppltn/1/5/{code}'
        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text)['SeoulRtd.citydata_ppltn'][0]
        print(contents)

        yield contents
