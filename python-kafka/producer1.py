from kafka import KafkaProducer
import json
import time

class Producer:
    def send_introduction(self, name, introduction):
        message = {
            "name": name,
            "introduction": introduction
        }

        # 각 인원의 자기소개를 Kafka 토픽으로 전송
        self.producer.send(self.topicName, json.dumps(message).encode("utf-8"))
        print(message)
        time.sleep(0.5)

    def done(self):
        # 데이터를 모두 보낸 후 종료 신호 보내기
        done_message = {"name": "DONE", "introduction": "실습 종료"}
        self.producer.send(self.topicName, json.dumps(done_message).encode("utf-8"))
        print("Sent done")

    def __init__(self, brokers, topicName):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            api_version=(0, 11, 5),
        )
        self.topicName = topicName

if __name__ == '__main__':
    brokers = ["localhost:9092"]
    topicName = "boaz"
    producer = Producer(brokers, topicName)

    introductions = [
        {"name": "엔지", "introduction": "보아즈 데이터 엔지니어링"},
        {"name": "이수민", "introduction": "노는 게 제일 좋아 친구들 모여라 언제나 즐거워~🥰"},
        {"name": "황혜정", "introduction": "최강 엔지 므쨍이 대표님"},
        {"name": "김주연", "introduction": "최강 분석 므쨍이 대표님"},
        {"name": "권준혁", "introduction": "솔직히 못하지만 저 그냥 노래 부르는 거 좋아해요. 그래서 1년 동안 합창단도 했어요. \n거기서 알았죠, 저는 음감이 딸린다는 것을 ㅋㅋ 다음에 코노 같이 가실 분들 같이 가요.\n저는 참고로 발라드 같은 거 불러요. 저 그리고 MBTI는 ESFP에요!"},
        {"name": "장경훈", "introduction": "개발을 좋아하는 사람, MBTI는 ESFP 입니다"},
        {"name": "노명은", "introduction": "- 뮤지컬 좋아합니다. 올해 10주년인 뮤지컬들이 많아서 캐스팅이 화려할 예정이니, 보러가시길 추천드립니다! \n- 지금 환승연애3를 시청하면서 작성하고 있습니다. 도파민 중독되고 있는 것 같습니다.."},
        {"name": "정진철", "introduction": "정.진.철 (IAM 정진철이에요)"},
        {"name": "김유빈", "introduction": "디저트를 정말 좋아합니다! 같이 디저트 도장깨기 하실 분을 언제나 찾고 있어요🥺\n코노 좋아합니다! 케이팝 같이 부르실 분들은 언제나 환영합니다,,,!"},
        {"name": "유태혁", "introduction": "해보고 싶은 것도 많고 새로운 도전을 하는 걸 좋아합니다!\n시간 나면 여행 가는 거 정말 좋아합니다!!"},
        {"name": "신예린", "introduction": "배우고 싶은 것도, 관심을 가지고 있는 것도, 좋아하는 것도 많은 사람입니다!\n요즘은 갑자기 피아노와 클래식에 관심이 생겨서 취미로 피아노를 배울까 고민하고 있습니다."},
        {"name": "신성준", "introduction": "영화, 넷플릭스 매우 좋아합니다. 플스, 닌텐도와 같은 콘솔 게임도 즐겨해요!\n 보드게임도 좋아합니다ㅎㅎ(ISFP라 실내 위주 활동 굿입니다)"},
        {"name": "최지혁", "introduction": "저의 MBTI는 ESFP 입니다! 전 21사단 신교대 조교 출신입니다!\n 음악을 무척 좋아합니다! 게임을 좋아합니다!"},
        {"name": "한상진", "introduction": "축구 보는 거, 하는 거 둘 다 좋아합니다! BOAZ 소모임으로 축구 소모임도 있던데 나중에 공 찰 기회가 있으면 좋겠습니다!! (잘하진 못하는데 열심히는 합니다 ㅎㅎ...)"},
        {"name": "김한수", "introduction": "최근에 인싸같다는 말을 가끔 듣는데 MBTI는 INFJ입니다 ㅠㅠ \n최근 2주 동안에 스키장을 3번 정도 다녀왔어요!\n최근에 반오십이 되어서 무의식적으로 큰일났다라는 생각이 덜컥 들었어요"},
        {"name": "이혜승", "introduction": "추천시스템, 이미지분석을 깊게 공부해보고 프로젝트를 진행해보고 싶어요!\n그리고 최근에는 인간-컴퓨터 상호작용(HCI) 이라는 분야에 관심이 생겨서 더 알아가보고 싶습니다 ㅎㅎ"},
        {"name": "이하윤", "introduction": "제 MBTI는 ESTP입니다 ! 하지만 요새 방학하고 집에만 있었더니 ISFP 된 것 같아여 물론 잇프피 친구한테 말했더니 절대 아니라고 욕먹었습니다 ㅠ.ㅠ T지만 공감 아주 잘하니 편하게 다가와주세요 ..~"},
        {"name": "원종빈", "introduction": "겁나 빠른 거북이하겠습니다.\n제 성격상 밑바닥부터 뭔가 파고드는 걸 높게 평가해서, 종종 남들보다 뒤쳐질 때가 있어요.\n그럴 때 마다  일케일케 천천히 가는 것이 결국에는 더 빠른 길이라고 믿고, 오늘도 기어가렵니다."},
        {"name": "김도환", "introduction": "열정맨..?\n시험기간엔 밤을 새서 공부하고, 운동 할 때는 힘들어 더이상 못 할 때까지 합니다 ㅋㅋ... \n후회하는 것을 싫어해서 할 때는 최대한 열심히 하려고 하는 것 같아요!"​},
        {"name": "DONE", "introduction": "실습 종료"}
    ]

    for person in introductions:
        producer.send_introduction(person["name"], person["introduction"])
    producer.done()
