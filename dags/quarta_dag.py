#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('quarta_dag', description='Minha Quarta Dag',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False) as dag:

    task_1 = BashOperator(task_id='quarta_dag__task_1', bash_command='sleep 5')
    task_2 = BashOperator(task_id='quarta_dag__task_2', bash_command='sleep 5')
    task_3 = BashOperator(task_id='quarta_dag__task_3', bash_command='sleep 5', trigger_rule='one_failed')

    task_3.set_upstream(task_2)
    task_2.set_upstream(task_1)
