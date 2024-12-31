from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# DAG definition
with DAG(
        dag_id='emr_spark_job_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
) as dag:

    # Step 1: Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides={
            "Name": "Airflow-EMR-Cluster",
            "ReleaseLabel": "emr-6.4.0",
            "Applications": [{"Name": "Spark"}],
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": "m5.xlarge",
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Worker nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": "m5.xlarge",
                        "InstanceCount": 2,
                    },
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
            },
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole",
        },
        aws_conn_id="aws_default",
    )

    # Step 2: Wait for the cluster to be ready
    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id='wait_for_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Step 3: Add a Spark job as a step
    spark_job_steps = [
        {
            "Name": "Run Spark Batch Job",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--class", "com.example.MySparkApp",
                    "s3://my-bucket/path/to/spark-job.jar",
                    "--input", "s3://my-bucket/input-data/",
                    "--output", "s3://my-bucket/output-data/",
                ],
            },
        }
    ]

    add_spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=spark_job_steps,
        aws_conn_id="aws_default",
    )

    # Step 4: Wait for the Spark step to complete
    wait_for_spark_step = EmrStepSensor(
        task_id='wait_for_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    # Step 5: Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Define task dependencies
    create_emr_cluster >> wait_for_emr_cluster >> add_spark_step >> wait_for_spark_step >> terminate_emr_cluster
