from faker import Faker
import random
import csv
from datetime import datetime
import time

class Usergenerator:
    def __init__(self):
        self.fake = Faker()
        self.DATA_SIZE =500
        self.fake.seed_instance(random.randint(1,9999))

    def make_profile(self):
        gender = random.choice(['M','F'])
        return self.fake.simple_profile(sex=gender)
        
    def write_csv(self):
        with open(f'../../spark/data/usersdata.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([ 'ID','username', 'name', 'sex', 'address', 'mail', 'birthdate'])
            for i in range(self.DATA_SIZE):
                temp_dict ={}
                temp_dict['ID'] = i+100001
                temp_dict.update(self.make_profile())
                writer.writerow(temp_dict.values())

item = Usergenerator()
item.write_csv()
