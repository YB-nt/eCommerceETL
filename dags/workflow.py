from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import psycopg2
from airflow.utils.task_group import TaskGroup


def check_table_count():
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        dbname='ecommerce_db',
        user='airflow',
        password='airflow'
    )

    cursor = conn.cursor()
    cursor.execute("""
                        SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables 
                        WHERE table_name = 'usersdata'
                    )
                    """)

    check = cursor.fetchone()[0]
    print("--"*50)
    print(check)
    print("--"*50)
    if check:
        return 'data_maker.make_group_task_start'
    else:
        return 'predata_maker.pre_make_group_task_start'


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

    start_task = DummyOperator(task_id='start_task', dag=dag)

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=check_table_count,
        dag=dag
    )
    
    with TaskGroup('predata_maker') as predata_maker:
        pre_make_group_task_start = DummyOperator(task_id='pre_make_group_task_start', dag=dag)
        bash_task_pre_1 = BashOperator(
            task_id='bash_task_1',
            bash_command='python /opt/airflow/dags/python_data_maker/generate_item.py'
        )
        bash_task_pre_2 = BashOperator(
            task_id='bash_task_2',
            bash_command='python /opt/airflow/dags/python_data_maker/generate_user.py'
        )
        bash_task_pre_3 = BashOperator(
            task_id='bash_task_3',
            bash_command='python /opt/airflow/dags/python_data_maker/pre_generate_log.py'
        )
        bash_task_pre_4 = BashOperator(
            task_id='bash_task_4',
            bash_command='python /opt/airflow/dags/python_data_maker/generate_rating.py'
        )
        pre_make_group_task_end = DummyOperator(task_id='make_group_task_end_1')
        pre_make_group_task_start >> [bash_task_pre_1,bash_task_pre_2,bash_task_pre_3,bash_task_pre_4] >> pre_make_group_task_end


    with TaskGroup('data_maker') as data_maker:
        make_group_task_start = DummyOperator(task_id='make_group_task_start', dag=dag)
        bash_task_1 = BashOperator(
            task_id='bash_task_1',
            bash_command='python /opt/airflow/dags/python_data_maker/generate_rating.py'
        )
        bash_task_2 = BashOperator(
            task_id='bash_task_2',
            bash_command='python /opt/airflow/dags/python_data_maker/generate_log.py'
        )
        make_group_task_end = DummyOperator(task_id='make_group_task_end_2', dag=dag)
        make_group_task_start >> [bash_task_1,bash_task_2] >> make_group_task_end
        
    predata_processing = BashOperator(
        task_id="predata_processing",
        bash_command="cd /usr/local && spark-submit --master spark://spark-master:7077 ./PreDataProcessing-assembly-0.1.jar",
        dag=dag
    )

    data_processing = BashOperator(
        task_id="data_processing",
        bash_command="cd /usr/local && spark-submit --master spark://spark-master:7077 ./DataProcessing-assembly-0.1.jar",
        dag=dag
    )

    end_task_1 = DummyOperator(task_id='end_task_1', dag=dag)
    end_task_2 = DummyOperator(task_id='end_task_2', dag=dag)

start_task >> branching  >> [data_maker,predata_maker]

make_group_task_end  >> data_processing >> end_task_1
pre_make_group_task_end >> predata_processing >> end_task_2
