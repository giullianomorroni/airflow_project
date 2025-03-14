#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

dag = DAG('group_dag', description='Grupos Dag',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False)

task_1 = BashOperator(task_id='primeira_dag__task_1', bash_command='sleep 5', dag= dag)
task_2 = BashOperator(task_id='primeira_dag__task_2', bash_command='sleep 5', dag= dag)
task_3 = BashOperator(task_id='primeira_dag__task_3', bash_command='sleep 5', dag= dag)

dag_group = TaskGroup('data_clean_group', dag=dag)

task_4 = BashOperator(task_id='primeira_dag__task_4', bash_command='sleep 5', dag= dag, task_group=dag_group)
task_5 = BashOperator(task_id='primeira_dag__task_5', bash_command='sleep 5', dag= dag, task_group=dag_group)

#sequencial
#task_1 >> task_2 >> task_3

#sequencial with parallel
task_1 >> [task_2, task_3]
task_3 >> dag_group