#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics

dag = DAG('dag_pandas',
          schedule=None, start_date=datetime(2025,1,1),
          catchup=False)

def clean():
    ds = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    ds.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio', 'Saldo', 'Produtos', 'Card Credito', 'Ativo', 'Salario', 'Saiu']
    ds['Salario'].fillna(statistics.median(ds['Salario']), inplace=True)
    ds['Genero'].fillna('Masculino', inplace=True)

    mdn = statistics.median(ds['Idade'])

    ds.loc[(ds['Idade'] < 0) | (ds['Idade'] > 120), 'Idade'] = mdn

    ds.drop_duplicates(subset='Id', keep='first', inplace=True)

    ds.to_csv('/opt/airflow/data/churn_cleaned.csv', sep=';', index=False)

task_1 = PythonOperator(task_id='dag_pandas__task_1', python_callable=clean, dag= dag)

#sequencial
task_1
