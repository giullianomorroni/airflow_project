#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

dag = DAG('dag_trigger_dag', description='Minha Primeira Dag',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False)

task_1 = BashOperator(task_id='dag_trigger_dag__task_1', bash_command='sleep 5', dag= dag)
task_2 = TriggerDagRunOperator(task_id='dag_trigger_dag__task_2', trigger_dag_id='sub_dag', dag=dag)

#sequencial
task_1 >> task_2
