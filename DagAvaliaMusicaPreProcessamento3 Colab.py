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
   'AvaliaMusica-PreProcessamento-Colab',
   schedule_interval=None,
   catchup=False,
   default_args=default_args
   ) as dag:

    start = DummyOperator(task_id="start")

        
    with TaskGroup("Init", tooltip="Limpeza") as init:
        t1 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f ./Analises/preprocessamento_colab.log
            touch ./Analises/preprocessamento_colab.log
            rm -f "./Analises/Histograma Users.pdf"
           
            """.format(pathScript)
        )
        t1        
        
    with TaskGroup("PreprocessaColab", tooltip="") as preproc:
        tRemoveUsuariosOutliers = BashOperator(
            dag=dag,
            task_id='Remove_Usuarios_Outliers',
            bash_command="""
            cd {0}
            python3 RemoveUsuariosOutliers.py
            """.format(pathScript)
        )
        tFiltraDomMusicasColab = BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas_Colab',
            bash_command="""
            cd {0}
            python3 FiltraDomMusicasColab.py
            """.format(pathScript)
        )
        tPreProcessaColab = BashOperator(
            dag=dag,
            task_id='Pre_Processa_Colab',
            bash_command="""
            cd {0}
            python3 PreProcessaColab.py
            """.format(pathScript)
        )
        tRemoveUsuariosOutliers >> tFiltraDomMusicasColab >> tPreProcessaColab
        
    with TaskGroup("ProcessaColab", tooltip="") as processa:
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 DescobreUsuariosVizinhos.py
            """.format(pathScript)
        )
        tProcessaColab        
         
    end = DummyOperator(task_id='end')
   
    start >> init >> preproc >> processa >> end
       