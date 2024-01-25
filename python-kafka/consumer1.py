from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, brokers, topicName):
        self.consumer = KafkaConsumer(
            topicName,
            group_id="consumer-group-v3",
            bootstrap_servers=brokers,
            api_version=(0, 11, 5),
        )

def receive_introduction(consumer, topic):
    for message in consumer:
        data = json.loads(message.value.decode())
        if data["name"] == topic:
            if data["name"] == "DONE":
                print("실습이 종료되었습니다.")
                break
            else:
                print(f"{data['name']}: {data['introduction']}")

if __name__ == "__main__":
    consumer = KafkaConsumer('boaz', bootstrap_servers='localhost:9092', group_id='introduction-group')

    print("자기소개를 출력할 이름을 입력하세요:")
    name = input()

    receive_introduction(consumer, name)
