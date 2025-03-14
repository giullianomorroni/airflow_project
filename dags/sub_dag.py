#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG('sub_dag', description='Minha Primeira Dag',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False)

task_1 = BashOperator(task_id='sub_dag__task_1', bash_command='sleep 5', dag= dag)
task_2 = BashOperator(task_id='sub_dag__task_2', bash_command='sleep 5', dag= dag)

#sequencial
task_1 >> task_2
