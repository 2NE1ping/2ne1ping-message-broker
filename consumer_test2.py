from flask import Flask, jsonify
from confluent_kafka import Consumer, KafkaError
import threading
import json

app = Flask(__name__)

# Kafka Consumer 설정
kafka_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'settings-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'security.protocol': 'PLAINTEXT'
}
consumer = Consumer(kafka_config)
consumer.subscribe(['test-topic'])

# 메시지 상태 저장 (전역 변수)
messages = {}

def consume_kafka_messages():
    """Kafka 메시지 실시간 소비"""
    global messages
    while True:
        msg = consumer.poll(1.0)  # 1초 대기하며 메시지 수신
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue

        # Kafka 메시지 처리
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {data}")
        messages[data['source']] = data

# Flask API
@app.route('/consume', methods=['GET'])
def get_messages():
    """저장된 메시지 반환"""
    global messages
    if 'A' in messages and 'B' in messages:
        combined_data = {
            'A': messages['A']['settings'],
            'B': messages['B']['settings']
        }
        # 상태 초기화
        messages = {}
        return jsonify({"status": "두 개의 메시지가 도착했습니다", "data": combined_data})
    else:
        return jsonify({"status": "메시지가 아직 충분하지 않습니다", "data": messages})

if __name__ == '__main__':
    # Kafka Consumer를 별도의 스레드로 실행
    threading.Thread(target=consume_kafka_messages, daemon=True).start()
    app.run(debug=True, port=5001)

