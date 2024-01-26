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

def receive_introduction(consumer, name):
    for message in consumer:
        data = json.loads(message.value.decode())
        if data["name"] == "DONE":
                break
        if data["name"] == name:
                print(f"{data['name']}: {data['introduction']}")
    print("--------------------")
    print("수고 많으셨습니다🙌")

if __name__ == "__main__":
    print("자기소개를 출력할 이름을 입력하세요:")
    name = input()
    print("--------------------")
    consumer = KafkaConsumer('boaz', bootstrap_servers='localhost:9092', group_id='consumer-group-v3')
    receive_introduction(consumer, name)
