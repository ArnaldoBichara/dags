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
            mkdir -p Analises
            """.format(pathScript)
        )
        t1 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f ./Analises/*.log
            rm -f ./Analises/Histograma*
            rm -f ./Analises/AudioFeatures.txt
            touch ./Analises/preprocessamento.log
           
            """.format(pathScript)
        )
        t0
        t1        
        

    with TaskGroup("ImportaDataSets", tooltip="Lê Datasets") as ledatasets:
        
        t1 = BashOperator(
            dag=dag,
            task_id='GetMusUsers',
            bash_command="""
            cd {0}
            python3 GetMusUsers.py
            """.format(pathScript)
        )
        tgetMusUserA = BashOperator(
            dag=dag,
            task_id='Import_UserA_do_Spotify',
            bash_command="""
            cd {0}
            python3 GetMusUserA.py
            """.format(pathScript)
        )        
        tgetFeatures = BashOperator(
            dag=dag,
            task_id='Get_Features_E_Dominio_Das_Musicas',
            bash_command="""
            cd {0}
            python3 GetFeaturesEDominioDasMusicas.py
            """.format(pathScript)
        ) 
        t1
        tgetMusUserA
        tgetFeatures

    with TaskGroup("Filtros", tooltip="Execução de filtros") as filtra:
        tfiltraUsers = BashOperator(
            dag=dag,
            task_id='Busca_MusUsers_No_Dominio',
            bash_command="""
            cd {0}
            python3 BuscaMusUsersNoDominio.py
            """.format(pathScript)
        )
        tAnalisaMusUsers = BashOperator(
            dag=dag,
            task_id='AnalisaMusUsers',
            bash_command="""
            cd {0}
            python3 AnalisaMusUsers.py
            """.format(pathScript)
        )
        tRemoveUsuariosOutliers = BashOperator(
            dag=dag,
            task_id='Remove_Usuarios_Outliers',
            bash_command="""
            cd {0}
            python3 RemoveUsuariosOutliers.py
            """.format(pathScript)
        )
        tfiltraUsers >> tAnalisaMusUsers >> tRemoveUsuariosOutliers
         

    end = DummyOperator(task_id='end')
   
    start >> init >> ledatasets >> filtra >> end

        