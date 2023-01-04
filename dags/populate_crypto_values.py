from airflow.models import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime
from pathlib import Path
import time as t
import requests
import json

with DAG(
    'populate_crypto_values',
    start_date=datetime.strptime('2021-01-01','%Y-%m-%d'),
    schedule_interval='0 7 * * *'
) as dag:
    path = Path(__file__).parent / '../google_client_oauth.txt'
    with open(path, 'r') as f:
        text = f.read()
        f.close()
    gsa = json.loads(text)

    crypto_id_list = ['bitcoin', 'ethereum']
    currency_id_list = ['eur', 'usd', 'brl']

    def get_client_bq_client():
        global client

        GCP_SERVICE_ACCOUNT_INFO = gsa

        SCOPES = ['https://www.googleapis.com/auth/bigquery']

        credentials = service_account.Credentials.from_service_account_info(GCP_SERVICE_ACCOUNT_INFO, scopes=SCOPES)

        client = bigquery.Client('data-case-study-322621', credentials=credentials)
        return client

    def get_crypto_values_func(crypto_id_list, currency_id_list, data_interval_end):
        crypto_id_list = crypto_id_list
        currency_id_list = currency_id_list

        url_base = 'https://api.coingecko.com/api/v3/coins/'
        price_dict_list = []

        for crypto_id in crypto_id_list:
            url = f'{url_base}{crypto_id}/history?date={data_interval_end}'
<<<<<<< HEAD
            r = requests.get(url)
=======
>>>>>>> Feat: Adds a new sql file, as well as completes the repository with the test logs
            get_crypto_bool = True 
            get_crypto_tries = 0

            while get_crypto_bool:
<<<<<<< HEAD
=======
                r = requests.get(url)
>>>>>>> Feat: Adds a new sql file, as well as completes the repository with the test logs
                if r.status_code == 200:
                    get_crypto_bool = False
                    body = json.loads(r.content.decode())
                    price_dict = {
                        'id': body['id'],
                        'symbol': body['symbol'],
                        'name': body['name'],
<<<<<<< HEAD
                        'snapshot_date': data_interval_end
=======
                        'snapshot_date': datetime.strptime(data_interval_end, '%d-%m-%Y').strftime('%Y-%m-%d')
>>>>>>> Feat: Adds a new sql file, as well as completes the repository with the test logs
                    }
                    for curr in currency_id_list:
                        if curr in body['market_data']['current_price']:
                            price_dict[f'current_price_{curr}'] = body['market_data']['current_price'][curr]

                        if curr in body['market_data']['market_cap']:
                            price_dict[f'market_cap_{curr}'] = body['market_data']['market_cap'][curr]


                    price_dict_list.append(price_dict)

                else:
<<<<<<< HEAD
                    if get_crypto_tries > 9:
                        get_crypto_bool = False
                        print(f'Could not find values for {crypto_id} in the desired date of {data_interval_end}')
                    get_crypto_tries +=1
                    t.sleep(30)

        client = get_client_bq_client()

        errors = client.insert_rows_json('data-case-study-322621.renan.test_table_crypto', price_dict_list)
=======
                    print(f'Attempt {get_crypto_tries + 1 } to get the crypto values has failed. The api status was {r.status_code}')
                    print(f'The api response was {r.content.decode()}')
                    if get_crypto_tries > 9:
                        get_crypto_bool = False
                        print(f'Could not find values for {crypto_id} in the desired date of {data_interval_end}')
                        raise Exception('Could not insert the values for this date')
                    get_crypto_tries +=1
                    t.sleep(15)

        client = get_client_bq_client()

        errors = client.insert_rows_json('data-case-study-322621.renan.crypto_currency', price_dict_list)
>>>>>>> Feat: Adds a new sql file, as well as completes the repository with the test logs
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors)) 
<<<<<<< HEAD
=======
            raise Exception('Could not insert the values for this date')
>>>>>>> Feat: Adds a new sql file, as well as completes the repository with the test logs

    get_crypto_values = PythonOperator(
        task_id = 'get_crypto_values',
        python_callable= get_crypto_values_func,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%d-%m-%Y")}}',
                    'crypto_id_list': crypto_id_list,
                    'currency_id_list': currency_id_list}
    )