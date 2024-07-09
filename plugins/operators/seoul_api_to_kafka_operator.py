from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from confluent_kafka import SerializingProducer

import configparser
import json

config = configparser.ConfigParser()
config.read('../../resources/config.ini')


def _get_area_code():
    area_code = config['API']['AreaCode'].split(',')
    return area_code


def _extract_data(date, contents):
    data = {
        'current_time': date,
        'area_name': contents['AREA_NM'],
        'area_code': contents['AREA_CD'],
        # 'live_population': contents['LIVE_PPLTN_STTS'],
        'area_congest_level': contents['AREA_CONGEST_LVL'],
        'area_congest_message': contents['AREA_CONGEST_MSG'],
        'area_population_min': contents['AREA_PPLTN_MIN'],
        'area_population_max': contents['AREA_PPLTN_MAX'],
        'male_population_rate': contents['MALE_PPLTN_RATE'],
        'female_population_rate': contents['FEMALE_PPLTN_RATE'],
        'population_rate_0': contents['PPLTN_RATE_0'],
        'population_rate_10': contents['PPLTN_RATE_10'],
        'population_rate_20': contents['PPLTN_RATE_20'],
        'population_rate_30': contents['PPLTN_RATE_30'],
        'population_rate_40': contents['PPLTN_RATE_40'],
        'population_rate_50': contents['PPLTN_RATE_50'],
        'population_rate_60': contents['PPLTN_RATE_60'],
        'population_rate_70': contents['PPLTN_RATE_70'],
        'resident_population_rate': contents['RESNT_PPLTN_RATE'],
        'non_resident_population_rate': contents['NON_RESNT_PPLTN_RATE']
    }

    return data


def _call_api():
    import requests

    headers = {'Content-Type': 'application/json',
               'charset': 'utf-8',
               'Accept': '*/*'}

    area_code = _get_area_code()
    api_key = config['API']['ApiKey']

    for code in area_code:
        request_url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/citydata_ppltn/1/5/{code}'
        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text)['SeoulRtd.citydata_ppltn'][0]

        yield contents


producer = SerializingProducer({
    'bootstrap.servers': config['KAFKA']['BootstrapServer']
})
topic = config['KAFKA']['Topic']


def _delivery_report(error, msg):
    if error is not None:
        print(f'Message delivery failed: {error}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def _kafka_publish(producer, topic, data):
    producer.produce(
        topic=topic,
        key=data['area_code'],
        value=json.dumps(data, ensure_ascii=False),
        on_delivery=_delivery_report
    )

    producer.poll(0)


if __name__ == '__main__':
    from datetime import datetime, timezone, timedelta

    KST = timezone(timedelta(hours=9))
    date = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

    for content in _call_api():
        data = _extract_data(date, content)
        _kafka_publish(producer, topic, data)

    producer.flush()
