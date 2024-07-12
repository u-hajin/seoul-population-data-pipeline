import json

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook


class SeoulApiToKafkaOperator(BaseOperator):
    template_fields = (
        'endpoint', 'kafka_config_id', 'topic', 'kafka_publish_key', 'dataset_name', 'area_code', 'current_time')

    def __init__(self, dataset_name, kafka_config_id, topic, kafka_publish_key, current_time=None, **kwargs):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul}}/json/' + self.dataset_name + '/1/5'
        self.kafka_config_id = kafka_config_id
        self.topic = topic
        self.kafka_publish_key = kafka_publish_key
        self.current_time = current_time
        self.area_code = '{{var.value.area_code}}'

    def execute(self, context):
        connection = BaseHook.get_connection(self.http_conn_id)
        base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        producer = KafkaProducerHook(kafka_config_id=self.kafka_config_id).get_producer()

        for content in self._call_api(base_url):
            data = self._extract_data(content)
            self._kafka_publish(producer, data)

        producer.flush()

    def _call_api(self, base_url):
        import requests

        self.area_code = self.area_code.split(',')

        for code in self.area_code:
            request_url = f'{base_url}/{code}'
            response = requests.get(request_url)
            contents = json.loads(response.text)['SeoulRtd.citydata_ppltn'][0]

            yield contents

    def _extract_data(self, content):
        extracted_data = {
            'area_name': content['AREA_NM'],
            'area_code': content['AREA_CD'],
            # 'live_population': content['LIVE_PPLTN_STTS'],
            'area_congest_level': content['AREA_CONGEST_LVL'],
            'area_congest_message': content['AREA_CONGEST_MSG'],
            'area_population_min': int(content['AREA_PPLTN_MIN']),
            'area_population_max': int(content['AREA_PPLTN_MAX']),
            'male_population_rate': float(content['MALE_PPLTN_RATE']),
            'female_population_rate': float(content['FEMALE_PPLTN_RATE']),
            'population_rate_0': float(content['PPLTN_RATE_0']),
            'population_rate_10': float(content['PPLTN_RATE_10']),
            'population_rate_20': float(content['PPLTN_RATE_20']),
            'population_rate_30': float(content['PPLTN_RATE_30']),
            'population_rate_40': float(content['PPLTN_RATE_40']),
            'population_rate_50': float(content['PPLTN_RATE_50']),
            'population_rate_60': float(content['PPLTN_RATE_60']),
            'population_rate_70': float(content['PPLTN_RATE_70']),
            'resident_population_rate': float(content['RESNT_PPLTN_RATE']),
            'non_resident_population_rate': float(content['NON_RESNT_PPLTN_RATE'])
        }

        if self.current_time is not None:
            data = {'current_time': self.current_time}
            data.update(extracted_data)
            return data
        else:
            return extracted_data

    def _delivery_report(self, error, msg):
        if error is not None:
            self.log.error(f'Message delivery failed: {error}')
        else:
            self.log.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def _kafka_publish(self, producer, data):
        producer.produce(
            self.topic,
            key=data[self.kafka_publish_key],
            value=json.dumps(data, ensure_ascii=False),
            on_delivery=self._delivery_report
        )
        producer.poll(0)
