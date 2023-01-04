from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from emr_serverless.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)


EMR_FUNDAMENTUS = Variable.get('EMR_FUNDAMENTUS')
S3_LOGS_BUCKET = Variable.get('S3_LOGS_BUCKET')


DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{S3_LOGS_BUCKET}/logs/"}
    },
}


with DAG(
    dag_id='analytical_dre',
    start_date=datetime(2022, 10, 6),
    schedule_interval='10 19 * * 1-5',
    catchup=False

) as dag:


    create_app = EmrServerlessCreateApplicationOperator(
        task_id='create_spark_app',
        job_type='SPARK',
        release_label='emr-6.8.0',
        config={'name': 'airflow-analytical-dre'},
    )

    application_id = create_app.output


    union_datasets = EmrServerlessStartJobOperator(
        task_id='dre_union',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3://fundamentus-codes/sparkFiles/dre_union.py',
                'sparkSubmitParameters': '--conf spark.submit.pyFiles=s3://fundamentus-codes/sparkFiles.zip',
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    pre_processing_dre_analytical = EmrServerlessStartJobOperator(
        task_id='dre_analytical',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3://fundamentus-codes/sparkFiles/dre_analytical.py',
                'sparkSubmitParameters':'--conf spark.submit.pyFiles=s3://fundamentus-codes/sparkFiles.zip --conf spark.archives=s3://fundamentus-codes/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.executor.instances=4 --conf spark.driver.cores=2 --conf spark.driver.memory=4g'
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    create_app >> union_datasets >> pre_processing_dre_analytical >> delete_app