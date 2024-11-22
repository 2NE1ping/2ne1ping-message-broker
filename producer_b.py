from confluent_kafka import Producer
import json

producer = Producer({
    'bootstrap.servers': '127.0.0.1:9092',  # 로컬 IP
    'security.protocol': 'PLAINTEXT'
})


def send_message(topic, message):
    producer.produce(topic, json.dumps(message).encode('utf-8'))
    producer.flush()


# B 서버에서 메시지 전송
send_message('test-topic', {'source': 'B', 'settings': '2'})

