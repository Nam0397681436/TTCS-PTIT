from airflow import DAG
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from datetime import datetime,timedelta,date
from airflow.decorators import dag, task
import json

# hàm tranform riêng cho API IBM 
def transform_IBM(data):
    symol='IBM'
    time_series=data.get('Time Series (5min)')
    result=[]
    for key, value in time_series.items():
        tm=key.split(' ')
        day_str=tm[0]
        # Convert YYYY-MM-DD to epoch days
        day_date = datetime.strptime(day_str, '%Y-%m-%d').date()
        epoch_date = date(1970, 1, 1)
        epoch_days = (day_date - epoch_date).days
        
        time=tm[1]
        key_new=symol+" "+key
        record={
           'stock_key':key_new,
           'symbol':symol,
           'day':day_str,
           'time':key,
           'open':value.get('1. open'),
           'high':value.get('2. high'),
           'low':value.get('3. low'),
           'close':value.get('4. close'),
           'volume':value.get('5. volume') 
        }
        result.append(record)

    return result    

# hàm tranform riêng cho TSCO
def transform_TSCO(data):
    symbol='TSCO'
    time_series=data.get('Time Series (Daily)')
    result=[]

    for key, value in time_series.items():
        # Convert YYYY-MM-DD to epoch days
        day_date = datetime.strptime(key, '%Y-%m-%d').date()
        epoch_date = date(1970, 1, 1)
        epoch_days = (day_date - epoch_date).days
        
        new_key=symbol+" "+key
        record={
            'stock_key':new_key,
            'symbol':symbol,
            'day':key,
            'open':value.get('1. open'),
            'high':value.get('2. high'),
            'low':value.get('3. low'),
            'close':value.get('4. close'),
            'volume':value.get('5. volume') 
        }
        result.append(record)

    return result    

default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=3),
}  

@dag(
    dag_id='get_api',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2025,2,2),
    default_args=default_args 
)

# dag chinh
def get_api():
    @task
    def get_api_IBM(**kawargs):
        url='https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=D9OSQPD13DL9XFJO'
        import requests

        response= requests.get(url=url)
        
        if response.status_code==200:
            data_json=response.json()
        
            kawargs['ti'].xcom_push(key='ibm',value=data_json)
        
        return data_json
    @task
    # lấy dữ liệu api chứng khoán TSCO
    def get_api_TSCO(**kawargs):

        url='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSCO.LON&interval=1min&outputsize=full&apikey=D9OSQPD13DL9XFJO'

        import requests

        response=requests.get(url=url)
        if response.status_code==200:
            data_json=response.json()
        
            kawargs['ti'].xcom_push(key='tsco',value=data_json)

        return data_json
    @task
    # transform data hai api ibm, tsco
    def transform(**kawargs):
        
        # lấy data từ xcom từ các task trên
 
        data_ibm= kawargs['ti'].xcom_pull(task_ids='get_api_IBM',key='ibm')
        result_ibm= transform_IBM(data_ibm)
        data_tsco=kawargs['ti'].xcom_pull(task_ids='get_api_TSCO',key='tsco')
        result_tsco=transform_TSCO(data_tsco)

        # đẩy từng mảng lưu trữ record giá chứng khoán vào xcom riêng

        kawargs['ti'].xcom_push(key='ibm',value=result_ibm)
        kawargs['ti'].xcom_push(key='tsco',value=result_tsco)

        print("success")

    @task
    def push_kafka_ibm(**kawargs):
        # Define schema for IBM stock data
        schema = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": False, "field": "stock_key"},
                    {"type": "string", "optional": False, "field": "symbol"},
                    {"type": "string", "optional": False, "field": "day"},
                    {"type": "string", "optional": False, "field": "time"},
                    {"type": "string", "optional": False, "field": "open"},
                    {"type": "string", "optional": False, "field": "high"},
                    {"type": "string", "optional": False, "field": "low"},
                    {"type": "string", "optional": False, "field": "close"},
                    {"type": "string", "optional": False, "field": "volume"}
                ],
                "optional": False,
                "name": "stock"
            }
        }

        def delivery_report(err, msg):
           if err is not None:
               print(f"Delivery failed: {err}")
           else:
               print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

        data_ibm = kawargs['ti'].xcom_pull(task_ids='transform', key='ibm')
        hook = KafkaProducerHook(kafka_config_id='connect_kafka')
        topic_ibm = 'stock_IBM'

        producer = hook.get_producer()
        for message_dict in data_ibm:
            # Create message with schema and payload
            message = {
                "schema": schema["schema"],
                "payload": message_dict
            }
            # Convert to JSON string and encode
            message_bytes = json.dumps(message).encode('utf-8')
            message_key = message_dict['stock_key'].encode('utf-8')

            producer.produce(topic=topic_ibm, key=message_key, value=message_bytes)
            producer.flush()

    @task
    def push_kafka_tsco(**kawargs):
        # Define schema for TSCO stock data
        schema = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": False, "field": "stock_key"},
                    {"type": "string", "optional": False, "field": "symbol"},
                    {"type": "string", "optional": False, "field": "day"},
                    {"type": "string", "optional": False, "field": "open"},
                    {"type": "string", "optional": False, "field": "high"},
                    {"type": "string", "optional": False, "field": "low"},
                    {"type": "string", "optional": False, "field": "close"},
                    {"type": "string", "optional": False, "field": "volume"}
                ],
                "optional": False,
                "name": "stock"
            }
        }

        def delivery_report(err, msg):
           if err is not None:
               print(f"Delivery failed: {err}")
           else:
               print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
        data_tsco = kawargs['ti'].xcom_pull(task_ids='transform', key='tsco')
        hook = KafkaProducerHook(kafka_config_id='connect_kafka')
        topic_tsco = 'stock_TSCO'

        producer = hook.get_producer()
        for message_dict in data_tsco:
            # Create message with schema and payload
            message = {
                "schema": schema["schema"],
                "payload": message_dict
            }
            # Convert to JSON string and encode
            message_bytes = json.dumps(message).encode('utf-8')
            message_key = message_dict['stock_key'].encode('utf-8')

            producer.produce(topic=topic_tsco, key=message_key, value=message_bytes)
            producer.flush()

    [get_api_IBM(),get_api_TSCO()] >> transform() >> [push_kafka_ibm(),push_kafka_tsco()]


get_api()

    
        