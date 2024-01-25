import csv
import json
import os
import time

from kafka import KafkaConsumer


class Consumer:
    def __init__(self, brokers, topicName):
        self.consumer = KafkaConsumer(
            topicName,
            group_id="consumer-group-v3",
            bootstrap_servers=brokers,
            api_version=(0, 11, 5),
        )

    def income_check(self):
        print("Start checking")
        new_data = []  # 새로운 데이터 저장
        for message in self.consumer:
            data = json.loads(message.value.decode())
            # print(data)
            # 종료 신호인 경우
            if data["row"][0] == "DONE":
                break
            # Dominant_Hand가 "Right"인 경우
            if "Right" in str(data["row"][29]):
                print("--Right Players--")
                # Rank, Year, Position, Name, Age, Games, Runs, Hits, Home_Runs, Strikeouts, Batting_Average, Dominant_Hand 정보만 저장
                new_row = [data["row"][0], data["row"][1], data["row"][2], data["row"][3], data["row"][4], data["row"][5], data["row"][8], data["row"][9], data["row"][10], data["row"][13], data["row"][18], data["row"][19], data["row"][29]]
                new_data.append(new_row)

                print(f'{data["index"]} {new_row.__str__()}')

                # csv 파일 업데이트
                file_name = "./right.csv"
                file_path = os.path.join(os.path.dirname(__file__), file_name)
                with open(file_path, "a", newline='') as f:
                    writer = csv.writer(f)
                    if os.stat(file_path).st_size == 0:  # 파일이 비어있으면 헤더 추가
                        writer.writerow(['Rank', 'Year', 'Position', 'Name', 'Age', 'Games', 'Runs', 'Hits', 'Home_Runs', 'Strikeouts', 'Batting_Average', 'Dominant_Hand'])
                    writer.writerow(new_row)


        print("End checking")


if __name__ == '__main__':
    brokers = ["localhost:9092"]
    topicName = "right"
    consumer = Consumer(brokers, topicName)
    consumer.income_check()
