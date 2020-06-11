import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
args = {
    'owner': 'kalai',
    'email': ['kalaiucd@gmail.com'],
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False
}
dag = DAG(dag_id='Simple_DAG', default_args=args, schedule_interval='@daily', concurrency=1, max_active_runs=1,
          catchup=False)
task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo Hello, This is my first DAG in Airflow!',
    dag=dag
)
task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo With two operators!',
    dag=dag
)
task_1 >> task_2
