from faker import Faker
import random
import csv
from datetime import datetime
import time

class RatingGenerator:
    def __init__(self):
        self.fake = Faker()
        self.DATA_SIZE =300000
        self.fake.seed_instance(random.randint(1,9999))

    def make_user(self):
        return random.randrange(1,9999)
    
    def make_item(self):
        return random.randrange(1,9999)
    
    def make_rating(self):
        self.rating = [x for x in range(1,6)]
        rating_weights = [10,10,25,25,30]
        
        apply_rating = random.choices(self.rating,weights=rating_weights)[0]

        return apply_rating
        
    
    def make_timestamp(self):
        make_date = self.fake.date_object()
        
        time_list = [[0,4],[4,6],[6,9],[9,11],[11,14],[14,16],[16,18],[18,21],[21,0]]
        weights = [1,6,10,19,11,11,17,17,8] 
        
        apply_time = random.choices(time_list ,weights=weights)[0]
        
        time_setting = [datetime.strptime(str(make_date)+' '+str(apply_time[0]) + ':00:00', '%Y-%m-%d %H:%M:%S'),datetime.strptime(str(make_date)+' '+str(apply_time[-1]) + ':00:00', '%Y-%m-%d %H:%M:%S')]
        return self.fake.date_between(time_setting[0],time_setting[-1])
        
    def generate_customer(self):
        return [self.make_user(), self.make_item(),self.make_rating(), self.make_timestamp()]
    
    def write_csv(self):
        with open(f'data/rating.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'ItemID', 'rating', 'timestamp'])
            for _ in range(self.DATA_SIZE):
                writer.writerow(self.generate_customer())

item = RatingGenerator()
item.write_csv()
