from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup

pathScript = "/home/arnaldo/airflow/dags/avaliamusica"
pathProducao = pathScript + "/deployProducao/APIAvaliaMusica"

default_args = {
   'owner': 'Arnaldo Almeida',
   'depends_on_past': False,
   'start_date': datetime(2021, 7, 25),
   'retries': 0,
   }

with DAG(
   'AvaliaMusica4Build',
   schedule_interval=None,
   catchup=False,
   default_args=default_args
   ) as dag:

    start = DummyOperator(task_id="start")
        
    with TaskGroup("Limpeza", tooltip="Limpeza") as init:
        t0 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f ./Analises/processamentoClassificacao.log
            rm -f ./deployProducao/APIAvaliaMusica/estatisticas.pickle
            rm -f ./deployProducao/APIAvaliaMusica/modeloClassif.*
            """.format(pathScript)
        )
        t0        
    with TaskGroup("ClassifTreinamento", tooltip="Treinamento do modelo de Classificacao") as ClassifTreinamento:
        t0= BashOperator(
            dag=dag,
            task_id='ClassifTreinamento',
            bash_command="""
            cd {0}
            python3 ClassifTreinamento.py
            """.format(pathScript)
        )
        t0
    with TaskGroup("DeployProd", tooltip="Faz deploy da API AvaliaMusica") as DeployProd:
        t0 = BashOperator(
            dag=dag,
            task_id='CopiaArqsProducao',
            bash_command="""
            cd {0}
            cp ./FeatureStore/DominioAudioFeatures.pickle ./deployProducao/APIAvaliaMusica/
            cp ./FeatureStore/MusCandidatasCurte.pickle ./deployProducao/APIAvaliaMusica/
            cp ./FeatureStore/MusCandidatasNaoCurte.pickle ./deployProducao/APIAvaliaMusica/    
            cp ./FeatureStore/modeloClassif.* ./deployProducao/APIAvaliaMusica/        
            """.format(pathScript)
        )
        t1 = BashOperator(
            dag=dag,
            task_id='BuildDockerImage',
            bash_command="""
            cd {0}
            docker build -t arnaldobichara/api_avaliamusica:latest .    
            docker push arnaldobichara/api_avaliamusica:latest
            """.format(pathProducao)
        )
        t0 >> t1     
    end = DummyOperator(task_id='end')

    start >> init >> ClassifTreinamento >> DeployProd >> end
