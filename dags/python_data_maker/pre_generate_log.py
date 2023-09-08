from faker import Faker
import random
import csv
from datetime import datetime
import logging


class Loggenerator:
    def __init__(self):
        self.fake = Faker()
        self.DATA_SIZE =200

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s [%(levelname)s] %(message)s',
                            handlers=[logging.StreamHandler(),
                                      logging.FileHandler('/usr/local/data/log/app.log')])

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
        weights = [1,6,10,19,11,11,17,17,8] 
        
        apply_time = random.choices(time_list ,weights=weights)[0]
        
        time_setting = [datetime.strptime(str(make_date)+' '+str(apply_time[0]) + ':00:00', '%Y-%m-%d %H:%M:%S'),datetime.strptime(str(make_date)+' '+str(apply_time[-1]) + ':00:00', '%Y-%m-%d %H:%M:%S')]
        return self.fake.date_between(time_setting[0],time_setting[-1])
    
    def get_preference(self):
        return random.randrange(1, 5)
    
    def target_item(self):
        return random.randrange(100001,100501)

    def generate_prelog(self):
        return [self.user_genre(), self.action_genre(), self.access_path(), self.timestamp(),self.get_preference(),self.target_item()]
    
    def write_csv(self):
        try:
            with open(f'/usr/local/data/log/log.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ID', 'Action', 'Access_path', 'timestamp','preference','ItemID'])
                for _ in range(self.DATA_SIZE):
                    writer.writerow(self.generate_prelog())
            logging.info("Successfully wrote to /usr/local/data/log/log.csv")
        
        except Exception as e:
            logging.error("Failed to write to /usr/local/data/log/log.csv", exc_info=True)

gen = Loggenerator()
gen.write_csv()