from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres  import PostgresOperator
import psycopg2

def predata_make():
    from python_data_maker import pre_generate_log,generate_item,generate_user

    pre_generate_log.Loggenerator().write_csv()
    # generate_item.Itemgenerator().write_csv()
    # generate_user.Usergenerator().write_csv()
    # generate_rating.RatingGenerator().write_dat()


def data_make():
    from python_data_maker import generate_log,generate_rating

    generate_log.Loggenerator().write_csv()
    # generate_rating.RatingGenerator().write_dat()



default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 9, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
    dag_id="eCommerce_ETL_dag",
    default_args=default_args,
    start_date=dt.datetime(2023, 9, 4),
    schedule_interval="@daily",
    catchup=False,
) as dag:



    make_group_task_start = DummyOperator(task_id='make_group_task_start', dag=dag)
    make_group_task_end_1 = DummyOperator(task_id='make_group_task_end_1', dag=dag)
    make_group_task_end_2 = DummyOperator(task_id='make_group_task_end_2', dag=dag)


    data_make = PythonOperator(
        task_id='data_make',
        python_callable=data_make,
        dag=dag
    )

    predata_make = PythonOperator(
        task_id='predata_make',
        python_callable=predata_make,
        dag=dag
    )

    
    def check_table_count():
    # PostgreSQL 연결 설정
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            dbname='ecommerce_db',
            user='airflow',
            password='airflow'
        )

        # 쿼리 실행
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        
        # 결과 가져오기
        count = cursor.fetchone()[0]

        # 테이블 갯수에 따라 분기 결정
        if count > 0:
            return 'data_make'
        else:
            return 'predata_make'

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=check_table_count,
        dag=dag
    )

    predata_processing = BashOperator(
        task_id="predata_processing",
        bash_command="cd /usr/local && spark-submit --master spark://spark-master:7077 ./PredataProcessing-assembly-0.1.jar",
        dag=dag
    )

    data_processing = BashOperator(
        task_id="data_processing",
        bash_command="cd /usr/local && spark-submit --master spark://spark-master:7077 ./DataProcessing-assembly-0.1.jar",
        dag=dag
    )


make_group_task_start >> branching >> [data_make, predata_make]

data_make >> make_group_task_end_1 >> data_processing
predata_make >> make_group_task_end_2 >> predata_processing