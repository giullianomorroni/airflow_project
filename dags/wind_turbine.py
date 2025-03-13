#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from datetime import datetime, timedelta

from airflow.decorators import task_group
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operatos.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json
import os

from scipy.special import parameters
from sqlalchemy.databases import postgres

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

def process_file(**kwargs):
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push('idtemp', data['idtemp'])
        kwargs['ti'].xcom_push('powerfactor', data['powerfactor'])
        kwargs['ti'].xcom_push('hydraulicpressure', data['hydraulicpressure'])
        kwargs['ti'].xcom_push('temperature', data['temperature'])
        kwargs['ti'].xcom_push('timestamp', data['timestamp'])

    os.remove(Variable.get('path_file'))

get_data = PythonOperator(task_id='get_data', python_callable=process_file, provide_context=True, dag=dag)

create_table = PostgresOperator(task_id='create_table', postgres_conn_id='postgres',
                                sql='''create table if not exists
                                        sensors (idtemp varchar, powerfactor varchar,
                                        hydraulicpressure varchar, temperature varchar,
                                        timestamp varchar);
                                    ''',
                                task_group=group_database,
                                dag=dag
                                )

insert_table = PostgresOperator(task_id='insert_table', postgres_conn_id='postgres',
                                     parameters=(
                                            '{{ ti.xcom_pull(task_id="get_data", key="idtemp") }}',
                                            '{{ ti.xcom_pull(task_id="get_data", key="powerfactor") }}',
                                            '{{ ti.xcom_pull(task_id="get_data", key="hydraulicpressure") }}',
                                            '{{ ti.xcom_pull(task_id="get_data", key="temperature") }}',
                                            '{{ ti.xcom_pull(task_id="get_data", key="timestamp") }}',
                                     ),
                                     sql='''INSERT INTO sensors (idtemp, powerfactor,
                                            hydraulicpressure, temperature, timestamp)
                                            VALUES (%s, %s, %s, %s, %s);''',
                                     task_group=group_database,
                                     dag=dag)


def check_temperature(**kwargs):
    value = kwargs['ti'].xcom_pull(task_id='get_data', key='temperature')
    if value is not None:
        value = float(value)
        print('Temperature', value)
        if value > 24.0:
            return 'group_check_temperature.send_email_alert'
        else:
            return 'group_check_temperature.send_email_normal'


send_email_alert = EmailOperator(task_id='send_email_alert', to='email@email.com', subject='Alerta Dados',
                                 html_content='Temperatura alta',
                                 task_group=group_check_temperature, dag=dag)

send_email_normal = EmailOperator(task_id='send_email_normal', to='email@email.com', subject='Alerta Dados',
                                 html_content='Tudo normal',
                                 task_group=group_check_temperature, dag=dag)

check_temperature_task = BranchPythonOperator(task_id='check_temperature', python_callable=check_temperature,
                                         provide_context=True, task_group=group_check_temperature, dag=dag)


with group_check_temperature:
    check_temperature_task >> [send_email_alert, send_email_normal]

with group_database:
    create_table >> insert_table

file_sensor_task >> get_data >> [group_check_temperature, group_database]