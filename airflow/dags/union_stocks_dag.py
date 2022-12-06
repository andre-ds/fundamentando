from datetime import datetime
from airflow import DAG
from airflow.models import Variable
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
    dag_id='union_stocks',
    start_date=datetime(2022, 12, 3),
    schedule_interval='@yearly',
    catchup=False

) as dag:


    create_app = EmrServerlessCreateApplicationOperator(
        task_id='create_spark_app',
        job_type='SPARK',
        release_label='emr-6.8.0',
        config={'name': 'union_stocks_all'},
    )

    application_id = create_app.output


    union_datasets = EmrServerlessStartJobOperator(
        task_id='stock_union_datasets_id',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3://fundamentus-codes/sparkFiles/pre_processing_stocks_all.py',
                'sparkSubmitParameters':'--conf spark.submit.pyFiles=s3://fundamentus-codes/sparkFiles.zip --conf spark.archives=s3://fundamentus-codes/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.executor.instances=4 --conf spark.driver.cores=2 --conf spark.driver.memory=4g',
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    create_app >> union_datasets >> delete_app