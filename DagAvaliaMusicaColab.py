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
   'AvaliaMusica-Colab',
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
            rm -f ./Analises/preprocessamentoColab.log
            touch ./Analises/preprocessamentoColab.log
           
            """.format(pathScript)
        )
        t1        
        
    with TaskGroup("ProcessaColab_50_150", tooltip="") as proc1:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 50 150
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        

    with TaskGroup("ProcessaColab_151_230", tooltip="") as proc2:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 151 230
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("ProcessaColab_231_350", tooltip="") as proc3:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 231 350
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("ProcessaColab_351_500", tooltip="") as proc4:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 351 500
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("ProcessaColab_501_800", tooltip="") as proc5:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 501 800
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("ProcessaColab_801_1500", tooltip="") as proc6:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 801 1500
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("ProcessaColab_1501_30000", tooltip="") as proc7:
        t0= BashOperator(
            dag=dag,
            task_id='Filtra_Faixa_De_Usuarios',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 1501 30000
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='Filtra_Dominio_Musicas',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='ColabPreprocessamento',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa_Colab',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
         
    end = DummyOperator(task_id='end')
   
    start >> init > proc1 > proc2 > proc3 > proc4 > proc5 > proc6 > proc7 >> end
