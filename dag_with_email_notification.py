from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

def send_email_notification(to_email, subject, body):
    send_email(to=to_email, subject=subject, html_content=body)

def success_notification():
    subject = 'Airflow Success Notification'
    body = 'The task finished successfully.'
    send_email_notification('mdarifishtiaq73@gmail.com', subject, body)

def failure_notification():
    subject = 'Airflow Failure Notification'
    body = 'The task failed.'
    send_email_notification('mdarifishtiaq73@gmail.com', subject, body)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'email_notification_example_python_operator',
    default_args=default_args,
    description='A DAG with email notifications using PythonOperator',
    schedule_interval=None,  # Set desired schedule interval
)

success_email_task = PythonOperator(
    task_id='success_email_task',
    python_callable=success_notification,
    dag=dag,
)

failure_email_task = PythonOperator(
    task_id='failure_email_task',
    python_callable=failure_notification,
    dag=dag,
)

success_email_task >> failure_email_task  # Define task dependencies#/
