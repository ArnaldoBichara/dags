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
   'AvaliaMusicaColab-Processamento',
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
            rm -f ./FeatureStore/ColabVizinhosUserA.pickle            
            rm -f ./FeatureStore/ColabVizinhosUserAbarra.pickle       
            """.format(pathScript)
        )
        t0        
        
    with TaskGroup("50_150", tooltip=" ") as proc1:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 50 150
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
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
        t0 >> t1 >> t2 >> tProcessa        

    with TaskGroup("151_230", tooltip="") as proc2:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 151 230
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("231_350", tooltip="") as proc3:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 231 350
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("351_500", tooltip="") as proc4:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 351 500
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("501_800", tooltip="") as proc5:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 501 800
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("801_1500", tooltip="") as proc6:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 801 1500
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab        
    with TaskGroup("1501_30000", tooltip="") as proc7:
        t0= BashOperator(
            dag=dag,
            task_id='FiltraUsers',
            bash_command="""
            cd {0}
            python3 ColabFiltraFaixaDeUsuarios.py 1501 30000
            """.format(pathScript)
        )
        t1= BashOperator(
            dag=dag,
            task_id='FiltraDomMus',
            bash_command="""
            cd {0}
            python3 ColabFiltraDomMusicas.py
            """.format(pathScript)
        )
        t2 = BashOperator(
            dag=dag,
            task_id='Preproc',
            bash_command="""
            cd {0}
            python3 Preproc.py
            """.format(pathScript)
        )
        tProcessaColab = BashOperator(
            dag=dag,
            task_id='Processa',
            bash_command="""
            cd {0}
            python3 ColabProcessaUsuariosVizinhos.py
            """.format(pathScript)
        )
        t0 >> t1 >> t2 >> tProcessaColab      
    
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

    start >> init >> proc1 >> proc2 >> proc3 >> proc4 >> proc5 >> proc6 >> proc7 >> montaMusColab >> end
