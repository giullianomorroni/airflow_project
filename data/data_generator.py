import json
from random import uniform
import time
from datetime import datetime

id = 0
while True:
    id += 1
    dados_pf = uniform(0.7, 1)

    dados_hp = uniform(70, 80)

    dados_tp = uniform(20, 25)
    registro = {'idtemp': str(id), 'powerfactor': str(dados_pf), 'hydraulicpressure': str(dados_hp),
                'temperature': str(dados_tp), 'timestamp': str(datetime.now())}

    with open('/Users/giullianomorroni/Development/airflow/data/data.json', 'w') as fp:
        json.dump(registro, fp)

    time.sleep(5000)
