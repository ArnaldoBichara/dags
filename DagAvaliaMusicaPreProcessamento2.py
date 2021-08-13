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
    'AvaliaMusica-PreProcessamento2',
#   schedule_interval=timedelta(minutes=60),
    schedule_interval=None,
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
            mkdir -p 'Resultado das Análises'
            """.format(pathScript)
        )
        t1 = BashOperator(
            dag=dag,
            task_id='Limpa_Logs',
            bash_command="""
            cd {0}
            rm -f './Resultado das Análises/PreProcessamento2.log'
            """.format(pathScript)
        )
        [t0, t1]
    
    with TaskGroup("Importa_Músicas", tooltip="Importa Músicas do Spotify") as importa_mus:
        
        tgetAudioFeatures = BashOperator(
            dag=dag,
            task_id='Get_AudioFeatures',
            bash_command="""
            cd {0}
            python3 GetAudioFeatures.py
            """.format(pathScript)
        )        
        tgetUserA = BashOperator(
            dag=dag,
            task_id='Import_UserA_do_Spotify',
            bash_command="""
            cd {0}
            python3 GetMusUserA.py
            """.format(pathScript)
        )
        tincluiAUsers = BashOperator(
            dag=dag,
            task_id='Inclui_UserA_em_Users',
            bash_command="""
            cd {0}
            python3 IncluiUserA_em_Users.py
            """.format(pathScript)
        )
        tgetAFeatures = BashOperator(
            dag=dag,
            task_id='Get_UserA_AudioFeatures',
            bash_command="""
            cd {0}
            python3 GetUserA_AudioFeatures.py
            """.format(pathScript)
        )
        tincluiAFeatures = BashOperator(
            dag=dag,
            task_id='Inclui_UserA_em_AudioFeatures',
            bash_command="""
            cd {0}
            python3 IncluiUserA_em_AudioFeatures.py
            """.format(pathScript)
        )

         
        tgetUserA >> tincluiAUsers
        tgetUserA >> tgetAFeatures
        tgetAFeatures >> tincluiAFeatures
        tgetAudioFeatures >> tincluiAFeatures

    with TaskGroup("Filtros", tooltip="Execução de filtros") as filtra:
        
        tfiltraFeatures = BashOperator(
            dag=dag,
            task_id='Filtra_AudioFeatures',
            bash_command="""
            cd {0}
            python3 FiltraAudioFeatures.py
            """.format(pathScript)
        )
        tfiltraFeatures

    with TaskGroup("Análises", tooltip="Análise de dados") as analisa:
        
        tAnalisaFeatures = BashOperator(
            dag=dag,
            task_id='Analisa_AudioFeatures',
            bash_command="""
            cd {0}
            python3 "Análise AudioFeatures.py"
            """.format(pathScript)
        )
        tAnalisaFeatures
        
    with TaskGroup("Normalização", tooltip="Normalização de dados") as normaliza:
        
        tNormalizaFeatures = BashOperator(
            dag=dag,
            task_id='Normaliza_AudioFeatures',
            bash_command="""
            cd {0}
            python3 "NormalizaAudioFeatures.py"
            """.format(pathScript)
        )
        tNormalizaFeatures
   
    end = DummyOperator(task_id='end')
    
    start >> init >> importa_mus >> filtra >> analisa >> normaliza >> end