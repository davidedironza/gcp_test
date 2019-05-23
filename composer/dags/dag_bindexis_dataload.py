
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""

from __future__ import print_function

import datetime
import pytz

from airflow import models
from airflow.models import Variable
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import email_operator
#from airflow.operators import docker_operator

# Installing a local Python library
from dependencies import def_bindexis_dataload

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'owner': 'CI',
    'depends_on_past': False,
    'start_date': pytz.utc.localize(datetime.datetime(2019, 5, 9, 10, 0)).astimezone(pytz.timezone("Europe/Zurich")),
    'end_date': datetime.datetime(2019, 5, 20),
    'email': ['davide.dironza@axa.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # general retries, can be changed by every task
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'bindexis_end2end',
        schedule_interval=datetime.timedelta(days=1), # or in cron Format
        default_args=default_dag_args) as dag:

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    bindexis_python = python_operator.PythonOperator(
        task_id='bindexis-dataload-start',
        python_callable=def_bindexis_dataload.bindexis_dataload,
        op_kwargs={'user_bindexis': Variable.get("user_bindexis"),
                    'pw_bindexis': Variable.get("password_bindexis")},
        retries=2)

    # Likewise, the goodbye_bash task calls a Bash script.
    end_bash = bash_operator.BashOperator(
        task_id='bindexis-end',
        bash_command='echo bindexis-dataload-end.')

    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, bindexis_python executes before end_bash.
    bindexis_python >> end_bash


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

