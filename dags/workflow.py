from python_data_maker import pre_generate_log,generate_log,generate_item,generate_rating,generate_user
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator  import SparkSubmitOperator 
from airflow.providers.postgres.operators.postgres  import PostgresOperator

def predata_make():
    pre_generate_log.Loggenerator().write_csv()
    generate_item.Itemgenerator().write_csv()
    generate_user.Usergenerator().write_csv()
    generate_rating.RatingGenerator().write_dat()


def data_make():
    generate_log.Loggenerator().write_csv()
    generate_rating.RatingGenerator().write_dat()



default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    dag_id="eCommerce_ETL_dag",
    default_args=default_args,
    start_date=datetime.datetime(2023, 8, 20),
    schedule_interval="@daliy",
    catchup=False,
) as dag:



    make_group_task_start = DummyOperator(task_id='make_group_task_start', dag=dag)
    make_group_task_end = DummyOperator(task_id='make_group_task_end', dag=dag)


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

    check_predata_table = PostgresOperator(
        task_id="check_predata_table",
        postgres_conn_id = "airflow",
        sql="SELECT tablename FROM pg_catalog.pg.tables;" ,
        dag=dag
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: 'data_processing' if check_predata_table.output else 'predata_processing',
        dag=dag
    )

    predata_processing = SparkSubmitOperator(
        task_id ="predata_processing",
        conn_id="spark_option_pre",
        application="../spark/PredataProcessing-assembly-0.1.jar",
        conf={'spark.master': 'spark://spark-master:7077'},
        dag=dag
    )
    data_processing = SparkSubmitOperator(
        task_id ="data_processing",
        conn_id="spark_option",
        application="../spark/DataProcessing-assembly-0.1.jar",
        conf={'spark.master': 'spark://spark-master:7077'},
        dag=dag
    )



check_predata_table >> make_group_task_start >> branching >> [data_make,predata_make]

data_make >> make_group_task_end >> data_processing
predata_make >> make_group_task_end >> predata_processing