import pendulum

from airflow import DAG
from operators.seoul_api_to_kafka_operator import SeoulApiToKafkaOperator

with DAG(
        dag_id='dags_seoul_api_to_kafka',
        schedule='*/20 * * * *',
        start_date=pendulum.datetime(2024, 7, 11,  tz='Asia/Seoul'),
        catchup=False
) as dag:
    seoul_population = SeoulApiToKafkaOperator(
        task_id='seoul_population',
        dataset_name='{{var.value.seoul_population_dataset}}',
        kafka_config_id='{{var.value.kafka_config_id}}',
        topic='{{var.value.seoul_population_topic}}',
        kafka_publish_key='{{var.value.kafka_publish_key}}',
        current_time='{{data_interval_end.in_timezone("Asia/Seoul") | ts}}'
    )
