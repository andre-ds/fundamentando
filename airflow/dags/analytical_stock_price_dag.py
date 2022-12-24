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
    dag_id='analytical_stock_price',
    start_date=datetime(2022, 10, 6),
    schedule_interval='10 18 * * 1-5',
    catchup=False

) as dag:


    create_app = EmrServerlessCreateApplicationOperator(
        task_id='create_spark_app',
        job_type='SPARK',
        release_label='emr-6.8.0',
        config={'name': 'airflow-stock-price'},
    )

    application_id = create_app.output


    stock_analytical_temp = EmrServerlessStartJobOperator(
        task_id='stock_analytical_temp_id',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3://fundamentus-codes/sparkFiles/stock_price_analytical_temp.py',
                'sparkSubmitParameters':'--conf spark.submit.pyFiles=s3://fundamentus-codes/sparkFiles.zip --conf spark.archives=s3://fundamentus-codes/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.executor.instances=10 --conf spark.driver.cores=4 --conf spark.driver.memory=26g',
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    stock_analytical = EmrServerlessStartJobOperator(
        task_id='stock_analytical_id',
        application_id=application_id,
        execution_role_arn=EMR_FUNDAMENTUS,
        job_driver={
            "sparkSubmit": {
                'entryPoint':'s3://fundamentus-codes/sparkFiles/stock_price_analytical.py',
                'sparkSubmitParameters':'--conf spark.submit.pyFiles=s3://fundamentus-codes/sparkFiles.zip --conf spark.archives=s3://fundamentus-codes/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executor.cores=4 --conf spark.executor.memory=26g --conf spark.executor.instances=30 --conf spark.driver.cores=4 --conf spark.driver.memory=26g',
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    create_app >> stock_analytical_temp >> stock_analytical >> delete_app