#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

args = {
    'depends_on_past' : False,
    'start_date': datetime(2025,1,1),
    'email' : ['giullianomorroni@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('default_args_dag', default_args=args,
          schedule='@hourly',
          catchup=False, default_view='graph', tags=['clean', 'finance'])

task_1 = BashOperator(task_id='default_args_dag__task_1', bash_command='sleep 5', dag= dag)
task_2 = BashOperator(task_id='default_args_dag__task_2', bash_command='sleep 5', dag= dag)
task_3 = BashOperator(task_id='default_args_dag__task_3', bash_command='sleep 5', dag= dag)

#sequencial
#task_1 >> task_2 >> task_3

#sequencial with parallel
task_1 >> [task_2, task_3]