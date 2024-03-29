import csv
import json
import time

from kafka import KafkaProducer


class Producer:
    def __init__(self, brokers, topicName):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            api_version=(0, 11, 5),
        )
        self.topicName = topicName

    def produce_player(self, filePath):
        with open(filePath, "r") as file:
            reader = csv.reader(file)
            headings = next(reader)
            # print(headings)
            for i, row in enumerate(reader):
                data = {
                    "index": i,
                    "row": row
                }
                self.producer.send(self.topicName, json.dumps(data).encode("utf-8"))
                print(data)
                time.sleep(0.1)
            # 데이터를 모두 보낸 후 종료 신호 보내기
            data = {"row": ["DONE"]}
            self.producer.send(self.topicName, json.dumps(data).encode("utf-8"))
            print(data)
        print("Sent done signal, exiting...")


if __name__ == '__main__':
    brokers = ["localhost:9092"]
    topicName = "switch"
    producer = Producer(brokers, topicName)
    filePath = "./SFG_batting.csv"
    producer.produce_player(filePath)
