from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from sqlalchemy import create_engine
import requests
import pandas as pd
from datetime import datetime, timedelta
from slack_messages import send_alerts

today = datetime.now() #Fetching today's date
your_db = 'YOUR_DB_CONNECTION'

# API endpoint from Bank Of Canada to get today's Exchange Rate
pub_api = f'https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json?start_date={today.date()}&end_date={today.date()}'

def _get_today_rates():
    response = requests.get(pub_api)
    if response.status_code != 200: #Validating response
        print("ERROR")
        return
    
    data = response.json()  
    
    # Extracting the the JSON response
    observations = data.get('observations', [])
    
    if observations:
        df = pd.DataFrame(observations)
        date = df['d'].values[0]
        
        # Extracting the currency values
        rates = {}
        for column in df.columns:
            if column != 'd':
                rates[column] = float(df[column][0]['v'])
        
        clean_df = pd.DataFrame(rates, index=[date])
        
        # Creating SQL-Engine to load the data into DB
        connection = MySqlHook.get_connection(your_db)  
        con_str = 'mysql://'+ str(connection.login)+':'+str(connection.password) + '@' + str(connection.host) +':'+str(connection.port)+'/'+str(connection.schema)
        engine = create_engine(con_str)
        clean_df['date']=today.date() # Adding date into the DF
        clean_df.to_sql('BOC_exchange_rates', engine, if_exists='append', index=False) # Lodading data to DB
    else:
        print("No data available")

    # Sending Alerts to slack channel
    send_alerts(':alert:',clean_df,clean_df.columns,today.date(),'Bank of Canada Exchange Rates','C05LJHX8EQZ')
    engine.dispose()  # Closing / killing  the engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,6,15),
    'retries': 2,
    'retry_delay': timedelta(minutes=60),
}

with DAG(
    'Daily_FX_BOC',
    tags=['Mihir','FX','Daily'],
    default_args=default_args,
    catchup= False,
    schedule_interval='30 20 * * 1-5', #As my Airflow and server works on UTC I am setting up time with respect to UTC
):
    pull_data = PythonOperator(
    task_id = 'get_today_rates',
    python_callable = _get_today_rates,
    provide_context=True,    
    )
    
pull_data # Can skip this step as we have only 1 task and no dependancy
