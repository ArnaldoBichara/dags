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
    'AvaliaMusica2-PreProcessamento',
#   schedule_interval=timedelta(minutes=60),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
    ) as dag:

    start = DummyOperator(task_id="start")
    
    with TaskGroup("Init", tooltip="Iniciações") as init:
        t0 = BashOperator(
            dag=dag,
            task_id='CriaDiretorios',
            bash_command="""
            cd {0}
            mkdir -p Analises
            """.format(pathScript)
        )
        t1 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f ./Analises/preprocessamento2.log
            touch ./Analises/preprocessamento.log
            rm -f ./Analises/Histograma*
            rm -f ./Analises/AudioFeatures.txt
            """.format(pathScript)
        )
        [t0, t1]
    with TaskGroup("Importa_Músicas", tooltip="Importa Músicas do Spotify") as importa_mus:
        
        tgetAFeatures = BashOperator(
            dag=dag,
            task_id='Get_UserA_AudioFeatures',
            bash_command="""
            cd {0}
            python3 GetUserA_AudioFeatures.py
            """.format(pathScript)
        )
        tgetASamples = BashOperator(
            dag=dag,
            task_id='Get_UserA_AudioSamples',
            bash_command="""
            cd {0}
            python3 GetUserA_AudioSamples.py
            """.format(pathScript)
        )
        tmontaEspectro = BashOperator(
            dag=dag,
            task_id='MontaEspectrogramas',
            bash_command="""
            cd {0}
            python3 MontaEspectrogramas.py
            """.format(pathScript)
        )
        tgetAFeatures
        tgetASamples >> tmontaEspectro
    with TaskGroup("Filtros", tooltip="Execução de filtros") as filtra:
        tAnaliseFeatures = BashOperator(
            dag=dag,
            task_id='Análise_AudioFeatures',
            bash_command="""
            cd {0}
            python3 'Análise AudioFeatures.py'
            """.format(pathScript)
        )
        tfiltraFeatures = BashOperator(
            dag=dag,
            task_id='Filtra_AudioFeatures',
            bash_command="""
            cd {0}
            python3 FiltraAudioFeatures.py
            """.format(pathScript)
        )
        tAnaliseFeatures >> tfiltraFeatures
    end = DummyOperator(task_id='end')
    
    start >> init >> importa_mus >> filtra >> end