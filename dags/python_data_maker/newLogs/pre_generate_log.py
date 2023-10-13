from faker import Faker
import random
import csv
from datetime import datetime,timedelta
import logging
from weighted_choice import WeightedChoice

class Loggenerator:
    def __init__(self):
        self.fake = Faker()
        self.DATA_SIZE =500

    def action_genre(self):
        actions = ['View', 'ItemSearch', 'Buy', 'AddtoCart']
        weights = [0.55, 0.125, 0.125, 0.15]  # 각 행위에 대한 가중치 설정
        return random.choices(actions, weights=weights)[0] 
    
    def access_path(self):
        return random.choice(['facebook', 'direct', 'instagram', 'google', 'naver', 'etc'])
    
    def user_genre(self):
        return random.randrange(100001,100501)
    
    def timestamp(self):

        make_date = self.fake.date_object() 

        time_list = [[0,4],[4,6],[6,9],[9,11],[11,14],[14,16],[16,18],[18,21],[21,0]]
        weights = [0.01, 0.06, 0.1, 0.19, 0.11, 0.11, 0.17 ,0.17 ,0.08]
        
        apply_time = WeightedChoice(time_list,weights).run()


        start_time = datetime.combine(make_date, datetime.min.time()) + timedelta(hours=apply_time[0])
        end_time = datetime.combine(make_date, datetime.min.time()) + timedelta(hours=apply_time[1])

        random_time = random.uniform(start_time.timestamp(), end_time.timestamp())

        fake_datetime = datetime.fromtimestamp(random_time).strftime("%d/%b/%Y:%H:%M:%S")

        return fake_datetime
        
    
    def get_preference(self):
        return random.randrange(1, 5)
    
    def target_item(self):
        return random.randrange(100001,100501)

    def generate_prelog(self):
        return [self.user_genre(), self.action_genre(), self.access_path(), self.timestamp(),self.get_preference(),self.target_item()]
    
    def write_csv(self):
        with open(f'log.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Action', 'Access_path', 'timestamp','preference','ItemID'])
            for _ in range(self.DATA_SIZE):
                writer.writerow(self.generate_prelog())
            

gen = Loggenerator()
gen.write_csv()