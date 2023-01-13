import airflow
import os 
# импортируем модуль os, который даёт возможность работы с ОС
# указание os.environ[…] настраивает окружение

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

# прописываем пути
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import sys


import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2022, 5, 15),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval=None,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='print_date',
    bash_command='spark-submit --master yarn --deploy-mode cluster partition.py {{ ds }} /user/master/data/events /user/andrew_0/data/events',
        retries=3,
        dag=dag
)

t1