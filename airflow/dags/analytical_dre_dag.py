from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from emr_serverless.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

# Replace these with your correct values
EMR_FUNDAMENTUS = 'arn:aws:iam::368300062869:role/EMR_FUNDAMENTUS'
S3_LOGS_BUCKET = 'fundamentus-codes'

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f's3a://{S3_LOGS_BUCKET}/logs/'}
    },
}

with DAG(
    dag_id='analytical_dre',
    start_date=datetime(2022, 10, 6),
    schedule_interval='10 18 * * 1-5',
    catchup=False

) as dag:

    create_app = EmrServerlessCreateApplicationOperator(
        task_id='create_spark_app',
        job_type='SPARK',
        release_label='emr-6.7.0',
        config={'name': 'airflow-test'},
    )

    application_id = create_app.output


    append_datasets = EmrServerlessStartJobOperator(
        task_id='start_job_1',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3a://fundamentus-codes/sparkFiles/dre_analytical.py',
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    create_app >> append_datasets >> delete_app