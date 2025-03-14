#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG('postgres_dag',
          schedule_interval=None, start_date=datetime(2025,1,1),
          catchup=False)

task_1 = PostgresOperator(task_id='create_table', postgres_conn_id='postgres', sql='create table if not exists teste(id int);', dag=dag)

task_2 = PostgresOperator(task_id='insert_table', postgres_conn_id='postgres', sql='insert into teste values (1);', dag=dag)

task_1 >> task_2
