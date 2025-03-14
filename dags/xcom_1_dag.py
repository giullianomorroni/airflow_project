#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.example_dags.example_task_group_decorator import end_task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG('xcom_dag',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False)

def start_task(**kwarg):
    kwarg['ti'].xcom_push(key='valor_xcom1', value='123')

def end_task(**kwarg):
    valor = kwarg['ti'].xcom_pull(key='valor_xcom1')
    print(valor)

task_1 = PythonOperator(task_id='primeira_dag__task_1', python_callable=start_task, dag=dag)
task_2 = BashOperator(task_id='primeira_dag__task_2', bash_command='sleep 5', dag=dag)
task_3 = PythonOperator(task_id='primeira_dag__task_3', python_callable=end_task, dag=dag)

#sequencial
task_1 >> task_2 >> task_3
