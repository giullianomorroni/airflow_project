#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operatos.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json
import os


default_args = {
    'depends_on_past': False,
    'email': 'email@email.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retrie_delay': timedelta(10)
}

dag = DAG('wind_turbine_dage',
          schedule_interval=None, start_date=datetime(2025,1,1),
          default_args=default_args,
          default_view='graph',
          catchup=False)

group_check_temperature = TaskGroup('group_check_temperature', dag=dag)
group_database = TaskGroup('group_database', dag=dag)


file_sensor_task = FileSensor(task_id='file_sensor_task', filepath=Variable.get('path_file'), fs_conn_id='udemy_project_local_file', poke_interval=10, dag=dag)