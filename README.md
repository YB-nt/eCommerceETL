# E-Commerce Data ETL 
python으로 만들어준 log,user,item,rating 데이터를 사용해서 ETL을 진행하는 프로젝트 

![제목 없음-2023-09-15-1742](https://github.com/YB-nt/eCommerceETL/assets/74981759/69a18fa1-dba1-49de-a6c0-86ee95fb30a0)

## Getting Started

```

docker-compose up -d

```

## Option  

Edit .env file 

```
./envset.sh
```

---

### Airflow 
![KakaoTalk_20230915_181304240](https://github.com/YB-nt/eCommerceETL/assets/74981759/f45b266e-65ce-4bfc-ab39-137db8911856)


- localhost:8080

#### Dag Graph
<img width="1050" alt="KakaoTalk_20230915_181231258" src="https://github.com/YB-nt/eCommerceETL/assets/74981759/b3bb9d16-6a6f-44df-acd9-a0d7d63eb425">


### Spark 
<img width="1680" alt="KakaoTalk_20230915_181115284" src="https://github.com/YB-nt/eCommerceETL/assets/74981759/305568f2-d7e9-43d0-bc7b-5fbd9fbac413">

- Product recommendation system using cosine similarity 
- localhost:7077
- localhost:8081

### Postgres
- localhost:5432

### Metabase
- Setting custom Data visualization<br>

![KakaoTalk_20230915_092626468](https://github.com/YB-nt/eCommerceETL/assets/74981759/1af27995-c275-4cc1-9312-a5c6d512b590)

- localhost:3000

  


---
### 추가 예정 

- AWS s3 연결 (airflow s3hook) 사용해서 데이터 생성 및 저장 
- 생성데이터 늘려주고, AWS EMR 으로 데이터 처리
- log 데이터 형식 변경 & 처리방식 변경
  - https://github.com/s4tori/fake-logs

