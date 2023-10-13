from datetime import datetime,timedelta
import random
from faker import Faker
from tzlocal          import get_localzone
from weighted_choice import WeightedChoice
import pandas as pd


"""
기존 data 생성과 동일하게 실행가능하도록 데이터 수정 
line 출력으로 raw 쌓아서 데이터 구성 가능하도록 수정 
필수 데이터 추가
*** init_protocol > init_referrer 순으로 실행  ***
"""


class Loggenerator:
    def __init__(self):
        self.faker = Faker()
        self.DATA_SIZE = 500
        self.dispatcher = {}
        self.date_pattern = "%d/%b/%Y:%H:%M:%S"
        self.sleep = None
        self.check_itme_id = None

    def readcsv_upload_category(self,id_value):
        df = pd.read_csv("itemsdata.csv"\
                ,names=['ID', 'Name', 'Price', 'Category'])

        return  df[df['ID']==id_value]['Category'].values[0]

    def readcsv_check_user_id(self):
        df = pd.read_csv("usersdata.csv"\
                ,names=['ID','username', 'name', 'sex', 'address', 'mail', 'birthdate'])
        random_id = random.choice(df['ID'])
        return  random_id
    def readcsv_check_item_id(self):
        df = pd.read_csv("itemsdata.csv"\
                ,names=['ID', 'Name', 'Price', 'Category'])
        random_id = random.choice(df['ID'])
        return  random_id
# ---------------------------------------------
    def init_date(self):
        make_date = self.faker.date_object() 

        time_list = [[0,4],[4,6],[6,9],[9,11],[11,14],[14,16],[16,18],[18,21],[21,0]]
        weights = [0.01, 0.06, 0.1, 0.19, 0.11, 0.11, 0.17 ,0.17 ,0.08]
        
        apply_time = WeightedChoice(time_list,weights).run()


        start_time = datetime.combine(make_date, datetime.min.time()) + timedelta(hours=apply_time[0])
        end_time = datetime.combine(make_date, datetime.min.time()) + timedelta(hours=apply_time[1])

        random_time = random.uniform(start_time.timestamp(), end_time.timestamp())

        fake_datetime = datetime.fromtimestamp(random_time).strftime("%d/%b/%Y:%H:%M:%S")
        timetemp ="["+str(fake_datetime)

        return timetemp

    def init_host(self):
        return self.faker.ipv4()
    
    def init_userid(self):
        return self.readcsv_check_user_id()

    def init_method(self):
        rng = WeightedChoice(["GET", "POST", "DELETE", "PUT"], [0.8, 0.1, 0.05, 0.05])
        return rng.run()

    def init_protocol(self):
        base_data = "HTTP/1.0"
        ran_quantity = random.randint(1,10)
        self.check_itme_id = self.readcsv_check_item_id()
        action = WeightedChoice(['View', 'ItemSearch', 'Buy', 'AddtoCart'],[0.55, 0.125, 0.125, 0.15]).run()
        if action =='ItemSearch':
           item_category = self.readcsv_upload_category(self.check_itme_id) 

           return f"search?q={item_category} {base_data}"
        
        else:
            return f"{action}?item_id={self.check_itme_id}&quantity={ran_quantity} {base_data}"

    def init_referrer(self):
        if self.check_itme_id is not None:
            referrer = self.faker.uri()+"Item?id="+self.check_itme_id
            return referrer
        else:
            return self.faker.uri()

    def init_server_name(self, servers=None):
        if servers is None:
            servers = ['Neptune', 'Aurora', 'Phoenix', 'Titan', 'Orion']
        return random.choice(servers)

    def init_size_object(self):
        return int(random.gauss(5000, 50))

    def init_status_code(self):
        rng = WeightedChoice(["200", "404", "500", "301"], [0.9, 0.04, 0.02, 0.04])
        return rng.run()

    def init_timezone(self):
        timezone = datetime.now(get_localzone()).strftime("%z")
        return f"{timezone}]"

    def init_url_request(self, list_files=None):
        if list_files is None:
            list_files = []
            for _ in range(0, 10):
                list_files.append(self.faker.file_path(depth=random.randint(0, 2), category="text"))

        return random.choice(list_files)

    def init_user_agent(self):
        user_agent = [self.faker.chrome(), self.faker.firefox(), self.faker.safari(), self.faker.internet_explorer(), self.faker.opera()]
        rng = WeightedChoice(user_agent, [0.5, 0.3, 0.1, 0.05, 0.05])
        return rng.run()
    

    # 192.30.253.112 - - [02/Jan/2018:20:59:51 +0100] "GET /dolorem/dicta.csv HTTP/1.0" 200 5039 "https://example1.com/" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_9) AppleWebKit/5351 (KHTML, like Gecko) Chrome/14.0.850.0 Safari/5351
    # 75.250.189.172 - 09/Jul/2004:16:13:46 100324 GET View?item_id=100026&quantity=8 HTTP/1.0 http://oconnor-fletcher.com/terms/Item?id=100026 example2 5005 200 +0900 /at/product.csv Mozilla/5.0 (X11; Linux i686) AppleWebKit/536.1 (KHTML, like Gecko) Chrome/50.0.851.0 Safari/536.1
    def generate_logs(self):
        """log 형식으로 데이터 작성해주기 """
        logsdata =[]
        logdata  = [self.init_host()
                    ,self.init_date()
                    ,self.init_timezone()
                    ,self.init_userid()
                    ,self.init_method()
                    ,self.init_protocol()
                    ,self.init_status_code() 
                    ,self.init_size_object()
                    ,self.init_referrer()
                    ,self.init_server_name()
                    ,self.init_url_request() 
                    ,self.init_user_agent()]
        for i in logdata:
            logsdata.append(str(i))
            if (logsdata[0]==i):
                logsdata.append("- -")
            
                
        rawdata = " ".join(logsdata)
        
        return rawdata
    
    def write_file(self):
        with open("ecommerce.log","w") as logfile:
           for i in range(self.DATA_SIZE):
               if(i%100==0):
                    print(f"[INFO] MADE DATA :{i} ")
               logfile.write(self.generate_logs()+'\n') 


en = Loggenerator()
en.write_file()
