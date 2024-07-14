# seoul-population-data-pipeline

## 프로젝트 소개

'서울시 실시간 인구' API를 호출해 서울시 주요 115개 장소에서의 데이터를 추출하고 kafka, spark를 통해 Amazon S3, Redshift에 저장합니다. Airflow를 사용해 자동으로 20분마다 데이터를 추출해 시간에 따른 장소 혼잡도 정도, 실시간 인구 지표, 성별과 나이에 따른 인구 비율 등을 저장하고 시각화합니다.

## 시스템 구조

![architecture](./images/architecture.png)

## 프로젝트 구성

### 환경 구축
- Docker compose 사용해 컨테이너 구성
- Airflow, Kafka, Spark(1 master, 3 worker) 컨테이너 실행 및 관리

### 데이터 추출
- 데이터셋 이름, kafka topic 이름 등 간단한 설정만 인자로 입력하면 동작하는 custom operator 제작
- API를 호출하고 추출한 데이터를 kafka로 publish하는 custom operator
- 서울시 도시 통합 데이터 API 호출에도 쓰일 수 있도록 제작해 재사용성 높임

### 데이터 변환
- Spark에서 kafka로부터 데이터를 소비
- DataFrame으로 변환 후 S3에 저장

### 데이터 저장
- 연도, 월, 일, 시, 분 기준 파티션을 나누어 S3에 저장
- Glue crawler를 사용해 S3 데이터를 스캔하고 메타데이터를 glue data catalog에 저장
- Redshift에 데이터를 이동시키지 않거나 이동시켜 조회하는 방법 2가지 학습 
  1. Redshift spectrum으로 외부 스키마를 생성해 데이터 이동시키지 않고 S3의 데이터 조회
  2. COPY 명령어를 통해 S3에 저장된 데이터를 redshift로 옮겨와 데이터 조회

### 데이터 시각화
- Query editor v2에서 제공하는 차트 기능으로 데이터를 시각화

## 개발 환경
- Docker compose
- Airflow: 2.9.2
- Scala: 2.12
- OS: Mac

## 실행 방법
1. `custom_image/airflow` 경로 내 Dockerfile로 아래 명령어를 통해 이미지를 생성합니다.  
`sudo docker build -t airflow_custom .`
2. 컨테이너 시작을 위해 `docker compose up -d`
3. Airflow UI `localhost:8080`에 접속해 Connections과 Variables를 설정해야 합니다. `images` 경로 내 이미지를 참고해주세요.  
API key는 열린 데이터 광장에서 직접 발급받아야 합니다.
4. `jobs/config.py`에 각 항목을 문자열로 작성합니다.
5. `docker exec -it seoul-population-data-pipeline-spark-master-1 spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark_seoul_population.py`를 입력해 spark job을 실행합니다.

## 서비스 UI 접속
- Airflow: localhost:8080
- Control Center: localhost:9021
- Spark: localhost:9090

## 실행 화면
![result_kafka](./images/result_kafka.png)  
![result_s3](./images/result_s3.png)  
![result_query_editor](./images/result_query_editor.png)  
![result_min](./images/result_min.png)  
![result_max](./images/result_max.png)  
![result_age_population](./images/result_age_population.png)  
![result_resident](./images/result_resident.png)
