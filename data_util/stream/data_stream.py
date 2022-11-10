from kafka import KafkaConsumer, KafkaProducer
from pathlib import Path
import os, time
# Create a consumer to read data from kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html

class PATH:
    path = Path('./').absolute()
    data_path = path/'data'
    log_name = 'kafka_log-movielog6.csv'
    log_fp = data_path/log_name
    
class kafka_streamer:
    def __init__(self, topic, servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            # Read from the start of the topic; Default is latest
            auto_offset_reset='earliest',
            # auto_offset_reset='latest',
            group_id='team06',
            # Commit that an offset has been read
            enable_auto_commit=True,
            # How often to tell Kafka, an offset has been read
            auto_commit_interval_ms=1000,
            consumer_timeout_ms=1000,
            # api_version=(2, 0, 2)
        )
        
    def stream_to_csv(self, n_msgs:int, log_fp=PATH.log_fp):        
        for n, message in enumerate(self.consumer):
            message = message.value.decode()
            if n==0: print(f"Test message:{message}")
            # Default message.value type is bytes!
            os.system(f"echo {message} >> {log_fp}")
            if n == n_msgs-1:break
        print('done')
        
    def stream_to_csv_ratings(self, n_msgs:int, log_fp=PATH.log_fp, sleepInterval=0.02):
        """ stream selectively ratings"""
        count_rate = 0
        for n, message in enumerate(self.consumer):
            message = message.value.decode()
            # Default message.value type is bytes!
            log_fp = Path(log_fp)
            if n ==0:
                print(f"Test message:{message}\ntype:{message.split(',')[-1].split('//s+')[-1].split('/')}", )
            try:
                if message.split(',')[-1].split('//s+')[-1].split('/')[1]!='data':
                    count_rate += 1
                    os.system(f"echo {message} >> {log_fp}")
                    if count_rate == 1: print(f"Rate Message: {message}")
                if count_rate >= n_msgs:
                    break
            except Exception as e:
                continue
                
        print('done')
        
    def close(self):
        self.consumer.close()
        
    # # Create a producer to write data to kafka
    # def write_to_stream(self):
    #     # Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    #     producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    #                              value_serializer=lambda x: dumps(x).encode('utf-8'),
    #                             )
    #     cities = ['Pittsburgh','New York','London','Bangalore','Shanghai','Tokyo','Munich']
    #     # Write data via the producer
    #     print("Writing to Kafka Broker")
    #     for i in range(10):
    #         data = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")},{cities[randint(0,len(cities)-1)]},{randint(18, 32)}ºC'
    #         print(f"Writing: {data}")
    #         producer.send(topic=topic, value=data)
    #         sleep(1)