import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime


def from_csv_to_dataframe(file):
    df = pd.read_csv(file, delimiter=",")

    return df


engine = create_engine('postgresql+psycopg2://postgres:admin@localhost/postgres')


def write_to_db(file, table_name):
    raw_df = from_csv_to_dataframe(file)
    raw_df.to_sql(table_name, con=engine, if_exists='append', index=False)


write_to_db('../transactions.csv', 'transactions')
write_to_db('../webinar.csv', 'webinar')
write_to_db('../users.csv', 'users')


def get_df_from_sql():
    script = """SELECT users.user_id, SUM(transactions.price) AS price_sum FROM users 
                JOIN transactions ON transactions.user_id = users.user_id 
                JOIN webinar ON webinar.email = users.email
                AND users.date_registration > '2016-04-01 00:00:00' 
                AND users.email NOT IN (SELECT users.email FROM users 
                WHERE users.date_registration < '2016-04-01 00:00:00')
                GROUP BY users.user_id;"""
    df = pd.read_sql(script, con=engine)

    return df


new_df = get_df_from_sql()


def write_to_final_table():
    new_df.to_sql('total_sum', con=engine, if_exists='append', index=False)


with DAG("my_dag", start_date=datetime(2022, 2, 9),
         schedule_interval='@once') as dag:
    task_1 = PythonOperator(
        task_id="get_db",
        python_callable=write_to_db
    )

    task_2 = PythonOperator(
        task_id='final_table',
        python_callable=write_to_final_table
    )

    task_1 >> task_2
