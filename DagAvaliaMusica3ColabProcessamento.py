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
   'AvaliaMusica3Colab-Processamento',
   schedule_interval=None,
   catchup=False,
   default_args=default_args
   ) as dag:

    start = DummyOperator(task_id="start")
        
    with TaskGroup("Init", tooltip="Limpeza") as init:
        t0 = BashOperator(
            dag=dag,
            task_id='Limpa_Arquivos',
            bash_command="""
            cd {0}
            rm -f ./Analises/processamentoColab.log
            rm -f ./Analises/ColabMusUserAEsparsa.pickle
            rm -f ./Analises/ColabMusUserAbarraEsparsa.pickle
            rm -f ./FeatureStore/ColabVizinhosUserA.pickle            
            rm -f ./FeatureStore/ColabVizinhosUserAbarra.pickle       
            """.format(pathScript)
        )
        t0        
    with TaskGroup("Filtra_Outliers", tooltip=" ") as filtra_outliers:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraUsuariosOutliers.py
            """.format(pathScript)
        )
        t0
    with TaskGroup("50-120", tooltip=" ") as proc1:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 50 120
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("121-200", tooltip=" ") as proc2:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 121 200
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("201-270", tooltip=" ") as proc3:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 201 270
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("271-400", tooltip=" ") as proc4:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 271 400
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("401-700", tooltip=" ") as proc5:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 401 700
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("701-1200", tooltip=" ") as proc6:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 701 1200
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("1201-2000", tooltip=" ") as proc7:    
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 ColabPreProcessamento.py 1201 2000
            """.format(pathScript)
        )
        tProcessa = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t2 >> tProcessa        
    with TaskGroup("MusCandidatas", tooltip="Monta Músicas Candidatas") as montaMusColab:
        t1 = BashOperator(
            dag=dag,
            task_id='Monta',
            bash_command="""
            cd {0}
            python3 ColabMontaMusCandidatas.py          
            """.format(pathScript)
        )
        t1     
    end = DummyOperator(task_id='end')

    start >> init >> filtra_outliers >> proc1 >> proc2 >> proc3 >> proc4 >> proc5 >> proc6 >> proc7 >> montaMusColab >> end
