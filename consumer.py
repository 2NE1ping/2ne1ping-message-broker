from flask import Flask, request, jsonify
from confluent_kafka import Consumer, KafkaError
import json
import requests

app = Flask(__name__)

# Kafka Consumer 설정
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'settings-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
consumer.subscribe(['settings-topic'])

# 메시지 상태 저장
messages = {}

@app.route('/consume', methods=['GET'])
def consume_messages():
    global messages

    while True:
        msg = consumer.poll(1.0)  # 1초 대기하며 메시지 수신
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # 메시지 처리
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {data}")
        messages[data['source']] = data

        # 두 메시지가 모두 도착했는지 확인
        if 'A' in messages and 'B' in messages:
            combined_data = {
                'A': messages['A']['settings'],
                'B': messages['B']['settings']
            }
            print(f"Both messages received: {combined_data}")

            # React 서버로 전송
            send_to_react_server(combined_data)

            # 상태 초기화
            messages = {}

    return jsonify({"status": "Consumer processed messages"})

def send_to_react_server(data):
    try:
        response = requests.post(
            "http://localhost:3000/api/settings",
            json=data
        )
        print(f"React server response: {response.status_code}")
    except Exception as e:
        print(f"Error sending to React server: {e}")

if __name__ == '__main__':
    app.run(debug=True, port=5001)

