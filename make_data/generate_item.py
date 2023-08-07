from faker import Faker
from faker.providers import BaseProvider
import random
import faker_commerce
import csv
from datetime import datetime
import time

class Itemgenerator:
    def __init__(self):
        self.fake = Faker()
        self.fake.add_provider(faker_commerce.Provider)
        self.fake.seed_instance(random.randint(1,9999))
        self.DATA_SIZE =100

    def item_name(self) -> str:
        return self.fake.ecommerce_name()
    # 최대한 원화 단위 맞춰주기 
    def item_price(self) -> int:
        return int(str(round(self.fake.ecommerce_price()/1000))+'0')
        
    def item_category(self) ->str:
        return self.fake.ecommerce_category()

    def generate_items(self):
        return [self.item_name(), self.item_price(), self.item_category()]
    
    def write_csv(self):
        with open(f'data/itemsdata.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Name', 'Price', 'Category'])
            for i in range(self.DATA_SIZE):
                item = self.generate_items() 
                
                item.insert(0,str(i+1).zfill(6))
                writer.writerow(item)


item = Itemgenerator()
item.write_csv()
"""
using https://github.com/nicobritos/python-faker-commerce
"""