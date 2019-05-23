
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""

from airflow import models
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.docker_operator import DockerOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'email': ['davide.dironza@axa.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('bindexis-end2end', default_args=default_args, schedule_interval=timedelta(days=1))


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)

# Send email confirmation
#email_summary = EmailOperator(
#    task_id='email_summary',
#    to=models.Variable.get('email'),
#    subject='ERROR: Bindexis Dataload and Trigger',
#    html_content="""
#    Bindexis Dataload fails.
#    Error: {ERROR_FROM_LOG}.
#    """.format(
#        ERROR_FROM_LOG=(
#            'CAN WE ACCESS THE LOGGING TEXT TO SHOW???'
#        )),
#    dag=dag)

