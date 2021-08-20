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
   schedule_interval=None,
   catchup=False,
   default_args=default_args
   ) as dag:

    start = DummyOperator(task_id="start")

        
    with TaskGroup("Init", tooltip="Cria Diretorios") as init:
        t0 = BashOperator(
            dag=dag,
            task_id='Cria_Diretorios',
            bash_command="""
            cd {0}
            mkdir -p FeatureStore
            mkdir -p "Resultado das Análises"
            """.format(pathScript)
        )
        t1 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f './Resultado das Análises/*.log'
            rm -f './Resultado das Análises/Histograma*'
            rm -f './Resultado das Análises/AudioFeatures.txt'
           
            """.format(pathScript)
        )
        [t0, t1]        
        

    with TaskGroup("ImportaDataSets", tooltip="Lê Datasets") as ledatasets:
        
        t1 = BashOperator(
            dag=dag,
            task_id='GetMusUsers',
            bash_command="""
            cd {0}
            python3 GetMusUsers.py
            """.format(pathScript)
        )
        tgetAudioFeatures = BashOperator(
            dag=dag,
            task_id='Get_AudioFeatures',
            bash_command="""
            cd {0}
            python3 GetAudioFeatures.py
            """.format(pathScript)
        ) 
        t1
        tgetAudioFeatures

    with TaskGroup("Filtros", tooltip="Execução de filtros") as filtra:
        tfiltraUsers = BashOperator(
            dag=dag,
            task_id='Filtra_Users',
            bash_command="""
            cd {0}
            python3 FiltraUsers.py
            """.format(pathScript)
        )
        tfiltraUsers        
        

    end = DummyOperator(task_id='end')
   
    start >> init >> ledatasets >> filtra >> end