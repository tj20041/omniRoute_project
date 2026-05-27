# # streaming final


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.models import Variable
from airflow.utils.email import send_email

from datetime import datetime, timedelta
import boto3
import logging

# ============================================================
# CONFIG
# ============================================================
REGION = "us-east-1"
CLUSTER_NAME_FILTER = "emr-cluster-deepak-07-04"

PRODUCER_SCRIPT = (
    "s3://ttn-de-bootcamp-silver-us-east-1/"
    "deepak-silver-07-04/scripts/tele_producer_to_bronze.py"
)

CLEANING_SCRIPT = (
    "s3://ttn-de-bootcamp-silver-us-east-1/"
    "deepak-silver-07-04/scripts/telemetry_data_cleaning2.py"
)

LOG_URI = (
    "s3://ttn-de-bootcamp-bronze-us-east-1/"
    "deepak-bronze-07-04/"
)

SUBNET_ID = "subnet-05a86391b0cc1b33d"
EC2_KEY = "harsh-ec2keypair"

MASTER_SG = "sg-0ec16a48a5c4876e3"
SLAVE_SG = "sg-064b9227088a5bb70"

EMAIL_LIST = [
    "harsh.vardhan1@tothenew.com",
    "deepak.bhatia@tothenew.com",
    "tanmay.joshi@tothenew.com"
]

default_args = {
    "owner": "deepak",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False
}

# ============================================================
# EMR CONFIG
# ============================================================
JOB_FLOW_OVERRIDES = {
    "Name": CLUSTER_NAME_FILTER,
    "ReleaseLabel": "emr-7.13.0",
    "LogUri": LOG_URI,

    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "Livy"},
        {"Name": "JupyterEnterpriseGateway"},
    ],

    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 15,
                                "VolumeType": "gp3"
                            },
                            "VolumesPerInstance": 1
                        }
                    ]
                }
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 4,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 15,
                                "VolumeType": "gp3"
                            },
                            "VolumesPerInstance": 1
                        }
                    ]
                }
            }
        ],

        "Ec2SubnetId": SUBNET_ID,
        "EmrManagedMasterSecurityGroup": MASTER_SG,
        "EmrManagedSlaveSecurityGroup": SLAVE_SG,
        "Ec2KeyName": EC2_KEY,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },

    "VisibleToAllUsers": True,
    "StepConcurrencyLevel": 5,

    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "AmazonEMRServiceRole",

    "Tags": [
        {"Key": "Project", "Value": "Bootcamp"},
        {"Key": "Environment", "Value": "POC"},
        {"Key": "Owner", "Value": "rahul.pupreja@tothenew.com"},
        {"Key": "CreatedBy", "Value": "deepak.bhatia@tothenew.com"},
        {"Key": "ManagedBy", "Value": "DataEngineering"},
        {"Key": "Name", "Value": "poc-bootcamp-emr-group4-streaming"}
    ]
}

# ============================================================
# FAILURE EMAIL ALERT
# ============================================================
def failure_email_alert(context):
    subject = f"Airflow Task Failed: {context['task_instance'].task_id}"

    body = f"""
    <h3>Airflow Task Failure Alert</h3>
    <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
    <p><strong>Task:</strong> {context['task_instance'].task_id}</p>
    <p><strong>Execution Date:</strong> {context['execution_date']}</p>
    <p>
        <a href="{context['task_instance'].log_url}">
            View Logs
        </a>
    </p>
    """

    send_email(
        to=EMAIL_LIST,
        subject=subject,
        html_content=body
    )


# ============================================================
# CHECK EMR HEALTH
# ============================================================
def check_emr_health(**context):
    client = boto3.client("emr", region_name=REGION)

    current_cluster_id = Variable.get(
        "emr_id",
        default_var=None
    )

    if not current_cluster_id:
        return "restart_if_failed"

    try:
        response = client.describe_cluster(
            ClusterId=current_cluster_id
        )

        state = response["Cluster"]["Status"]["State"]

        logging.info(f"Current cluster state: {state}")

        if state in ["RUNNING", "WAITING"]:
            logging.info("Cluster already running")
            return "end"
        else:
            return "restart_if_failed"

    except Exception as e:
        logging.error(f"Error checking cluster: {e}")
        return "restart_if_failed"


# ============================================================
# STORE NEW CLUSTER ID
# ============================================================
def update_cluster_id(**context):
    cluster_id = context["ti"].xcom_pull(
        task_ids="restart_if_failed"
    )

    Variable.set("emr_id", cluster_id)

    logging.info(f"Stored new cluster ID: {cluster_id}")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="dag1_streaming_monitoring_harshTest",
    start_date=datetime(2026, 5, 7),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    on_failure_callback=failure_email_alert
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_cluster = BranchPythonOperator(
        task_id="check_emr_health",
        python_callable=check_emr_health
    )

    restart_cluster = EmrCreateJobFlowOperator(
        task_id="restart_if_failed",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default"
    )

    update_cluster = PythonOperator(
        task_id="update_cluster_id",
        python_callable=update_cluster_id
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_ingestion_steps",
        job_flow_id="{{ var.value.emr_id }}",
        aws_conn_id="aws_default",
        steps=[
            {
                "Name": "Kafka_to_Bronze",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                        "org.postgresql:postgresql:42.7.3",
                        PRODUCER_SCRIPT
                    ]
                }
            },
            {
                "Name": "Bronze_to_Silver",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                        "org.postgresql:postgresql:42.7.3",
                        CLEANING_SCRIPT
                    ]
                }
            }
        ]
    )

    wait_for_cleaning = EmrStepSensor(
        task_id="wait_for_cleaning",
        job_flow_id="{{ var.value.emr_id }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_ingestion_steps')[1] }}",
        poke_interval=30,
        aws_conn_id="aws_default",
        target_states=["RUNNING"]
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # Flow
    start >> check_cluster

    # If cluster already running
    check_cluster >> end

    # If cluster needs restart
    check_cluster >> restart_cluster
    restart_cluster >> update_cluster
    update_cluster >> add_steps
    add_steps >> wait_for_cleaning >> end
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.providers.amazon.aws.operators.emr import (
#     EmrAddStepsOperator,
#     EmrCreateJobFlowOperator
# )
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
# from airflow.models import Variable
# from airflow.utils.email import send_email
# from datetime import datetime, timedelta
# import boto3
# import logging

# # ============================================================
# # CONFIG
# # ============================================================
# REGION = "us-east-1"
# CLUSTER_NAME_FILTER = "emr-cluster-deepak-07-04"

# PRODUCER_SCRIPT = (
#     "s3://ttn-de-bootcamp-bronze-us-east-1/"
#     "deepak-bronze-07-04/scripts/bronze_kafka_stream.py"
# )

# CLEANING_SCRIPT = (
#     "s3://ttn-de-bootcamp-silver-us-east-1/"
#     "deepak-silver-07-04/scripts/telemetry_data_cleaning2.py"
# )

# BOOTSTRAP_SCRIPT = (
#     "s3://ttn-de-bootcamp-bronze-us-east-1/"
#     "deepak-bronze-07-04/scripts/bootstrap.sh"
# )

# LOG_URI = (
#     "s3://ttn-de-bootcamp-bronze-us-east-1/"
#     "deepak-bronze-07-04/"
# )

# # Your infra config
# SUBNET_ID = "subnet-05a86391b0cc1b33d"
# EC2_KEY = "harsh-ec2keypair"

# MASTER_SG = "sg-0ec16a48a5c4876e3"
# SLAVE_SG = "sg-064b9227088a5bb70"

# EMAIL_LIST = [
#     "harsh.vardhan1@tothenew.com",
#     "deepak.bhatia@tothenew.com",
#     "tanmay.joshi@tothenew.com"
# ]

# default_args = {
#     "owner": "deepak",
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
#     "email": EMAIL_LIST,
#     "email_on_failure": True,
#     "email_on_retry": False
# }

# # ============================================================
# # NEW EMR CREATION CONFIG (LIKE TEAMMATE)
# # ============================================================
# JOB_FLOW_OVERRIDES = {
#     "Name": CLUSTER_NAME_FILTER,
#     "ReleaseLabel": "emr-7.13.0",
#     "LogUri": LOG_URI,

#     "Applications": [
#         {"Name": "Spark"},
#         {"Name": "Hadoop"},
#         {"Name": "Hive"},
#         {"Name": "Livy"},
#         {"Name": "JupyterEnterpriseGateway"},
#     ],

#     "Instances": {
#         "InstanceGroups": [
#             {
#                 "Name": "Primary",
#                 "Market": "ON_DEMAND",
#                 "InstanceRole": "MASTER",
#                 "InstanceType": "m5a.xlarge",
#                 "InstanceCount": 1,
#                 "EbsConfiguration": {
#                     "EbsBlockDeviceConfigs": [
#                         {
#                             "VolumeSpecification": {
#                                 "SizeInGB": 15,
#                                 "VolumeType": "gp3"
#                             },
#                             "VolumesPerInstance": 1
#                         }
#                     ]
#                 }
#             },
#             {
#                 "Name": "Core",
#                 "Market": "ON_DEMAND",
#                 "InstanceRole": "CORE",
#                 "InstanceType": "m5a.xlarge",
#                 "InstanceCount": 4,
#                 "EbsConfiguration": {
#                     "EbsBlockDeviceConfigs": [
#                         {
#                             "VolumeSpecification": {
#                                 "SizeInGB": 15,
#                                 "VolumeType": "gp3"
#                             },
#                             "VolumesPerInstance": 1
#                         }
#                     ]
#                 }
#             }
#         ],

#         "Ec2SubnetId": SUBNET_ID,
#         "EmrManagedMasterSecurityGroup": MASTER_SG,
#         "EmrManagedSlaveSecurityGroup": SLAVE_SG,
#         "Ec2KeyName": EC2_KEY,
#         "KeepJobFlowAliveWhenNoSteps": True,
#         "TerminationProtected": False
#     },

#     "VisibleToAllUsers": True,
#     "StepConcurrencyLevel": 5,

#     "JobFlowRole": "EMR_EC2_DefaultRole",
#     "ServiceRole": "AmazonEMRServiceRole",

#     # FIXED TAGS
#     "Tags": [
#         {
#             "Key": "Project",
#             "Value": "Bootcamp"
#         },
#         {
#             "Key": "Environment",
#             "Value": "POC"
#         },
#         {
#             "Key": "Owner",
#             "Value": "rahul.pupreja@tothenew.com"
#         },
#         {
#             "Key": "CreatedBy",
#             "Value": "deepak.bhatia@tothenew.com"
#         },
#         {
#             "Key": "ManagedBy",
#             "Value": "DataEngineering"
#         },
#         {
#             "Key": "Name",
#             "Value": "poc-bootcamp-emr-group4-streaming"
#         }
#     ],

#     # "BootstrapActions": [
#     #     {
#     #         "Name": "Bootstrap install dependencies",
#     #         "ScriptBootstrapAction": {
#     #             "Path": BOOTSTRAP_SCRIPT,
#     #             "Args": []
#     #         }
#     #     }
#     # ]
# }

# # ============================================================
# # EMAIL ALERT
# # ============================================================
# def failure_email_alert(context):
#     subject = f"Airflow Task Failed: {context['task_instance'].task_id}"

#     body = f"""
#     <h3>Airflow Task Failure Alert</h3>
#     <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
#     <p><strong>Task:</strong> {context['task_instance'].task_id}</p>
#     <p><strong>Execution Date:</strong> {context['execution_date']}</p>
#     <p>
#         <a href="{context['task_instance'].log_url}">
#             View Logs
#         </a>
#     </p>
#     """

#     send_email(
#         to=EMAIL_LIST,
#         subject=subject,
#         html_content=body
#     )


# # ============================================================
# # CHECK CLUSTER HEALTH
# # ============================================================
# def check_emr_health(**context):
#     client = boto3.client("emr", region_name=REGION)

#     current_cluster_id = Variable.get(
#         "emr_id",
#         default_var=None
#     )

#     if not current_cluster_id:
#         return "restart_if_failed"

#     try:
#         response = client.describe_cluster(
#             ClusterId=current_cluster_id
#         )

#         state = response["Cluster"]["Status"]["State"]

#         logging.info(f"Current cluster state: {state}")

#         if state in ["RUNNING", "WAITING"]:
#             logging.info("Cluster already running → end")
#             return "end"

#         else:
#             return "restart_if_failed"

#     except Exception:
#         return "restart_if_failed"


# # ============================================================
# # SAVE NEW CLUSTER ID
# # ============================================================
# def update_cluster_id(**context):
#     cluster_id = context["ti"].xcom_pull(
#         task_ids="restart_if_failed"
#     )

#     Variable.set("emr_id", cluster_id)

#     logging.info(
#         f"Stored new cluster ID: {cluster_id}"
#     )


# # ============================================================
# # DAG
# # ============================================================
# with DAG(
#     dag_id="dag1_streaming_monitoring_harshTest",
#     start_date=datetime(2026, 5, 7),
#     schedule="*/10 * * * *",
#     catchup=False,
#     default_args=default_args,
#     on_failure_callback=failure_email_alert
# ) as dag:

#     start = EmptyOperator(task_id="start")

#     check_cluster = BranchPythonOperator(
#         task_id="check_emr_health",
#         python_callable=check_emr_health
#     )

#     # NEW CLUSTER CREATION
#     restart_cluster = EmrCreateJobFlowOperator(
#         task_id="restart_if_failed",
#         job_flow_overrides=JOB_FLOW_OVERRIDES,
#         aws_conn_id="aws_default"
#     )

#     update_cluster = PythonOperator(
#         task_id="update_cluster_id",
#         python_callable=update_cluster_id
#     )

#     add_steps = EmrAddStepsOperator(
#         task_id="add_ingestion_steps",
#         job_flow_id="{{ var.value.emr_id }}",
#         aws_conn_id="aws_default",
#         steps=[
#             {
#                 "Name": "Kafka_to_Bronze",
#                 "ActionOnFailure": "CONTINUE",
#                 "HadoopJarStep": {
#                     "Jar": "command-runner.jar",
#                     "Args": [
#                         "spark-submit --deploy-mode cluster --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.7.3 s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/scripts/tele_producer_to_bronze.py
                        
#                     ]
#                 }
#             },
#             {
#                 "Name": "Bronze_to_Silver",
#                 "ActionOnFailure": "CONTINUE",
#                 "HadoopJarStep": {
#                     "Jar": "command-runner.jar",
#                     "Args": [
#                         spark-submit --deploy-mode cluster --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.7.3 s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/scripts/telemetry_data_cleaning2.py 
#                     ]
#                 }
#             }
#         ]
#     )

#     wait_for_cleaning = EmrStepSensor(
#         task_id="wait_for_cleaning",
#         job_flow_id="{{ var.value.emr_id }}",
#         step_id="{{ task_instance.xcom_pull(task_ids='add_ingestion_steps')[1] }}",
#         poke_interval=30,
#         aws_conn_id="aws_default",
#         target_states = ['RUNNING']
#     )

#     end = EmptyOperator(
#         task_id="end",
#         trigger_rule="none_failed_min_one_success"
#     )

#     # FLOW
#     start >> check_cluster

#     # already running → skip
#     check_cluster >> end

#     # terminated/not found → create new cluster
#     check_cluster >> restart_cluster >> update_cluster >> add_steps

#     add_steps >> wait_for_cleaning >> end
