from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from check_mongodb import check_mongodb_main
from kafka_producer import kafka_producer_main
from check_cassandra import check_cassandra_main
from kafka_create_topic import kafka_create_topic_main
from kafka_consumer_mongodb import kafka_consumer_mongodb_main
from kafka_consumer_cassandra import kafka_consumer_cassandra_main

start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

email_cassandra = check_cassandra_main()['email']
otp_cassandra = check_cassandra_main()['otp']
email_mongodb = check_mongodb_main()['email']
otp_mongodb = check_mongodb_main()['otp']


def decide_branch():
    create_topic = kafka_create_topic_main()
    if create_topic == "Created":
        return "topic_created"
    else:
        return "topic_already_exists"


with DAG('mongodb_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_new_topic = BranchPythonOperator(
        task_id='create_new_topic', python_callable=decide_branch)

    kafka_consumer_cassandra = PythonOperator(task_id='kafka_consumer_cassandra', python_callable=kafka_consumer_cassandra_main,
                                              retries=2, retry_delay=timedelta(seconds=1),
                                              execution_timeout=timedelta(seconds=20))

    kafka_consumer_mongodb = PythonOperator(task_id='kafka_consumer_mongodb', python_callable=kafka_consumer_mongodb_main,
                                            retries=2, retry_delay=timedelta(seconds=1),
                                            execution_timeout=timedelta(seconds=20))

    kafka_producer = PythonOperator(task_id='kafka_producer', python_callable=kafka_producer_main,
                                    retries=2, retry_delay=timedelta(seconds=1),
                                    execution_timeout=timedelta(seconds=20))

    check_cassandra = PythonOperator(task_id='check_cassandra', python_callable=check_cassandra_main,
                                     retries=2, retry_delay=timedelta(seconds=1),
                                     execution_timeout=timedelta(seconds=20))

    check_mongodb = PythonOperator(task_id='check_mongodb', python_callable=check_mongodb_main,
                                   retries=2, retry_delay=timedelta(seconds=1),
                                   execution_timeout=timedelta(seconds=20))

    topic_created = DummyOperator(task_id="topic_created")

    topic_already_exists = DummyOperator(task_id="topic_already_exists")

    send_email_cassandra = EmailOperator(
        task_id='send_email_cassandra',
        to=email_cassandra,
        subject='One-Time-Password',
        html_content=f"""
                <html>
                <body>
                <h1>Your OTP</h1>
                <p>{otp_cassandra}</p>
                </body>
                </html>
                """
    )

    send_email_mongodb = EmailOperator(
        task_id='send_email_mongodb',
        to=email_mongodb,
        subject='One-Time-Password',
        html_content=f"""
                <html>
                <body>
                <h1>You can find your One Time Password below</h1>
                <p>{otp_mongodb}</p>
                </body>
                </html>
                """
    )

    create_new_topic >> [topic_created, topic_already_exists] >> kafka_producer
    kafka_consumer_cassandra >> check_cassandra >> send_email_cassandra
    kafka_consumer_mongodb >> check_mongodb >> send_email_mongodb
