#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator

dag =  DAG('bigdata',
        schedule_interval=None,start_date=datetime(2025,1,1),
        catchup=False)

big_data = BigDataOperator(
        task_id="big_data",
        path_to_csv_file = '/opt/airflow/data/churn.csv',
        path_to_save_file = '/opt/airflow/data/churn.json',
        file_type = 'json',
        dag=dag
        )

task_big_data