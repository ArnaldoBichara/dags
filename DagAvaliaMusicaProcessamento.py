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
   'AvaliaMusica-Processamento',
#   schedule_interval=timedelta(minutes=60),
   schedule_interval=None,
   catchup=False,
   default_args=default_args
   ) as dag:

   start = DummyOperator(task_id="start")
        
   with TaskGroup("ImportaMusUserA", tooltip="Importa UserA do Spotify") as importausera:
        
        tgetUserA = BashOperator(
            dag=dag,
            task_id='ImportSpotifyUserA',
            bash_command="""
            cd {0}
            python3 GetMusUserA.py
            """.format(pathScript)
        )
        tincluiAUsers = BashOperator(
            dag=dag,
            task_id='IncluiUserA_em_Users',
            bash_command="""
            cd {0}
            python3 IncluiUserA_em_Users.py
            """.format(pathScript)
        )
        tgetAFeatures = BashOperator(
            dag=dag,
            task_id='GetUserA_AudioFeatures',
            bash_command="""
            cd {0}
            python3 GetUserA_AudioFeatures.py
            """.format(pathScript)
        )
        tincluiAFeatures = BashOperator(
            dag=dag,
            task_id='IncluiUserA_em_AudioFeatures',
            bash_command="""
            cd {0}
            python3 IncluiUserA_em_AudioFeatures.py
            """.format(pathScript)
        )
        tfiltraFeatures = BashOperator(
            dag=dag,
            task_id='FiltraAudioFeatures',
            bash_command="""
            cd {0}
            python3 FiltraAudioFeatures.py
            """.format(pathScript)
        )
        tlimpa = BashOperator(
            dag=dag,
            task_id='LimpaArquivosIntermediarios',
            bash_command="""
            
            cd {0}
            "rm FeatureStore/MusicasUserA.pickle
            """.format(pathScript)
        )
        
        tgetUserA >> tincluiAUsers
        tgetUserA >> tgetAFeatures
        tgetAFeatures >> tincluiAFeatures
        tincluiAFeatures >> tfiltraFeatures
        tincluiAUsers >> tlimpa
        tgetAFeatures >> tlimpa
        tincluiAFeatures >> tlimpa
        

   end = DummyOperator(task_id='end')
   start >> importausera >> end