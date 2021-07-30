from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup

pathScript = "/home/arnaldo/airflow/dags/avaliamusica"

default_args = {
   'owner': 'Arnaldo Almeida',
   'depends_on_past': False,
   'start_date': datetime(2021, 7, 25),
   'retries': 0,
   }

with DAG(
   'AvaliaMusica-PreProcessamento',
   schedule_interval=timedelta(minutes=60),
   catchup=False,
   default_args=default_args
   ) as dag:

   start = DummyOperator(task_id="start")

        
   with TaskGroup("Init", tooltip="Cria Diretorios") as init:
        t0 = BashOperator(
            dag=dag,
            task_id='CriaDiretorios',
            bash_command="""
            cd {0}
            mkdir -p FeatureStore
            """.format(pathScript)
        )
        t0

   with TaskGroup("ImportaDataSets", tooltip="Lê Datasets") as ledatasets:
        
        t1 = BashOperator(
            dag=dag,
            task_id='GetMusUsers',
            bash_command="""
            cd {0}
            python3 GetMusUsers.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='GetAudioFeatures',
            bash_command="""
            cd {0}
            python3 GetAudioFeatures.py
            """.format(pathScript)
        )
        [t1,t2]


   end = DummyOperator(task_id='end')
   start >> init >> ledatasets >> end