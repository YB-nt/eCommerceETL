from faker import Faker
from faker.providers import BaseProvider,date_time
import random
import csv
from datetime import datetime
import time

"""
페이지 방문 - 50~60%
사용자 인증 (반복적으로 확인) - 10~20 %
구매 5~10%
카트 15~20 %
검색 10~15%

"""
class Loggenerator:
    def __init__(self):
        self.fake = Faker()
        self.DATA_SIZE =200

    def action_genre(self):
        actions = ['View', 'ItemSearch', 'Buy', 'AddtoCart']
        weights = [0.55, 0.125, 0.125, 0.15]  # 각 행위에 대한 가중치 설정
        return random.choices(actions, weights=weights)[0] 
    
    def access_path(self):
        return random.choice(['facebook', 'direct', 'instagram', 'google', 'naver', 'etc'])
    
    def user_genre(self):
        temp_id = self.fake.unique.random_number(digits=6)
        format_Id = str(temp_id).zfill(6)
        return format_Id
    
    def timestemp(self):
        weights_time = random.randint(0,59)
        time = datetime(now.year, now.month, now.day, hour=10, minute=30)
        return self.fake.date_time_this_decade()
    
    def get_preference(self):
        return random.randrange(1, 10)

    def generate_customer(self):
        return [self.user_genre(), self.action_genre(), self.access_path(), self.timestemp(),self.get_preference()]
    
    def write_csv(self):
        with open(f'log_{time.time()}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Action', 'Access_path', 'timestamp','preference'])
            for _ in range(self.DATA_SIZE):
                writer.writerow(self.generate_customer())


gen = Loggenerator()
gen.write_csv()