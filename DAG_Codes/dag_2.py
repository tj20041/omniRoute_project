# #batch final
# from airflow import DAG
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.operators.python import BranchPythonOperator, PythonOperator
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime, timedelta
# from airflow.models import Variable
# import logging
# from datetime import timedelta

# # ============================================================
# # CONFIGURATION
# # ============================================================
# # The cluster ID for EMR operations [cite: 35]
# EMR_CLUSTER_ID = Variable.get("emr_id")

# # Arguments for spark-submit tasks [cite: 33]
# SPARK_ARGS = [
#     "spark-submit",
#     "--deploy-mode", "cluster",
#     "--packages",
#     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.7.3"
# ]

# default_args = {
#     "owner": "deepak",
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }

# # ============================================================
# # HELPER FUNCTIONS
# # ============================================================

# def start_logging():
#     logging.info("Batch Pipeline Started")

# def choose_flow(**context):
#     """Branches the DAG based on the current UTC execution hour."""
#     hour = context["execution_date"].hour
#     if hour == 0:
#         return "run_daily_silver"
#     elif hour == 5:
#         return "run_fuel_silver"
#     return "skip"

# def maintenance_check(**context):
#     """Triggers maintenance tasks only on January 1st."""
#     dt = context["execution_date"]
#     if dt.month == 1 and dt.day == 1:
#         return "run_maintenance"
#     return "skip_maintenance"

# def monthly_check(**context):
#     """Triggers monthly reporting only on the 1st day of the month."""
#     if context["execution_date"].day == 1:
#         return "monthly_report"
#     return "skip_monthly"

# # def get_latest_dag1_execution(execution_date, **context):
# #     # round to nearest previous 10-minute slot
# #     minute = (execution_date.minute // 10) * 10
    
# #     return execution_date.replace(
# #         minute=minute,
# #         second=0,
# #         microsecond=0
# #     )

# def get_latest_dag1_execution(execution_date, **context):
#     minute = (execution_date.minute // 10) * 10
    
#     rounded_time = execution_date.replace(
#         minute=minute,
#         second=0,
#         microsecond=0
#     )
    
#     # go to previous completed run
#     return rounded_time - timedelta(minutes=10)

# # ============================================================
# # DAG DEFINITION
# # ============================================================

# with DAG(
#     dag_id="dag2_batch_pipeline_harsh",
#     start_date=datetime(2026, 4, 29),
#     schedule="0 0,5 * * *",
#     catchup=False,
#     default_args=default_args
# ) as dag:

#     # # Dependency Sensor: Wait for Streaming DAG health check
#     # wait_for_dag1 = ExternalTaskSensor(
#     #     task_id="wait_for_dag1",
#     #     external_dag_id="dag1_streaming_monitoring_harsh",
#     #     external_task_id="check_emr_health",
#     #     allowed_states=["success"],
#     #     timeout=600
#     # )

#     wait_for_dag1 = ExternalTaskSensor(
#         task_id="wait_for_dag1",
#         external_dag_id="dag1_streaming_monitoring_harsh",
#         external_task_id="check_emr_health",
#         allowed_states=["success"],
#         execution_date_fn=get_latest_dag1_execution,
#         poke_interval=30,
#         timeout=600,
#         mode="reschedule"
#     )

#     start_task = PythonOperator(task_id="start", python_callable=start_logging)

#     branch = BranchPythonOperator(task_id="branch", python_callable=choose_flow)

#     skip = EmptyOperator(task_id="skip")

#     # ========================================================
#     # SILVER LAYER PIPELINES
#     # ========================================================

#     run_daily_silver = EmptyOperator(task_id="run_daily_silver")

#     # Master list of fleet vehicles [cite: 13, 120]
#     vehicle_registry = GlueJobOperator(
#         task_id="vehicle_registry_silver",
#         job_name="dim_vehicle",
#         region_name="us-east-1"
#     )

#     # Tracking driver transitions and rates [cite: 13, 126]
#     vehicle_assignment = GlueJobOperator(
#         task_id="vehicle_assignment_silver",
#         job_name="vehicle_assignment_clean",
#         region_name="us-east-1"
#     )

#     run_fuel_silver = EmptyOperator(task_id="run_fuel_silver")

#     # Logs for fuel quantity and odometer readings [cite: 13, 139]
#     fuel_silver = GlueJobOperator(
#         task_id="fuel_transactions_silver",
#         job_name="fuel_transactions_clean",
#         region_name="us-east-1"
#     )

#     # SENSOR: Cross-run check
#     # Ensures the 00:00 UTC registry run is successful before fuel processing
#     wait_for_registry_00_00 = ExternalTaskSensor(
#         task_id="wait_for_registry_00_00",
#         external_dag_id="dag2_batch_pipeline",
#         external_task_id="vehicle_registry_silver",
#         execution_delta=timedelta(hours=5),
#         allowed_states=["success"],
#         timeout=3600,
#         poke_interval=60,
#         mode="reschedule"
#     )

#     # ========================================================
#     # MAINTENANCE WORKFLOW
#     # ========================================================

#     maintenance_branch = BranchPythonOperator(
#         task_id="maintenance_check",
#         python_callable=maintenance_check
#     )

#     run_maintenance = EmptyOperator(task_id="run_maintenance")

#     # Mandatory downtime for vehicles [cite: 13, 133]
#     maintenance_silver = GlueJobOperator(
#         task_id="maintenance_silver",
#         job_name="dim_maintenance",
#         region_name="us-east-1"
#     )

#     skip_maintenance = EmptyOperator(task_id="skip_maintenance")

#     # CRITICAL: Join task for maintenance branches
#     maintenance_join = EmptyOperator(
#         task_id="maintenance_join", 
#         trigger_rule="none_failed_min_one_success"
#     )

#     # ========================================================
#     # GOLD LAYER PIPELINE (EMR/SPARK)
#     # ========================================================

#     start_gold_pipeline = EmptyOperator(task_id="start_gold_pipe_line")

#     # Violation Events: Flagging speed or geofence breaches [cite: 24]
#     violation = EmrAddStepsOperator(
#         task_id="violation_events_gold",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Violation Events", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table1_violation_events.py"]}}]
#     )
#     violation_sensor = EmrStepSensor(task_id="wait_violation", job_flow_id=EMR_CLUSTER_ID, step_id="{{ task_instance.xcom_pull('violation_events_gold')[0] }}")

#     # Driver Safety: Tracking strikes and penalized rates [cite: 25, 26]
#     driver = EmrAddStepsOperator(
#         task_id="driver_safety_gold",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Driver Safety", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table2_driver_safety_status.py"]}}]
#     )
#     driver_sensor = EmrStepSensor(task_id="wait_driver", job_flow_id=EMR_CLUSTER_ID, step_id="{{ task_instance.xcom_pull('driver_safety_gold')[0] }}")

#     # Asset History: SCD Type 2 tracking [cite: 7, 15]
#     asset = EmrAddStepsOperator(
#         task_id="asset_history_gold",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Asset History", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table3_vehicle_driver_history.py"]}}]
#     )
#     asset_sensor = EmrStepSensor(task_id="wait_asset", job_flow_id=EMR_CLUSTER_ID, step_id="{{ task_instance.xcom_pull('asset_history_gold')[0] }}")

#     # Fleet Snapshot: Tracking in-transit vehicles [cite: 19, 81]
#     fleet = EmrAddStepsOperator(
#         task_id="fleet_snapshot_gold",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Fleet Snapshot", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table5_active_fleet_snapshot.py"]}}]
#     )
#     fleet_sensor = EmrStepSensor(task_id="wait_fleet", job_flow_id=EMR_CLUSTER_ID, step_id="{{ task_instance.xcom_pull('fleet_snapshot_gold')[0] }}")

#     # Fuel Efficiency: Identifying consumption outliers [cite: 8, 21]
#     fuel_gold = EmrAddStepsOperator(
#         task_id="fuel_efficiency_gold",
#         # Ensures fuel audit runs even when yearly maintenance is skipped
#         trigger_rule = "none_failed_min_one_success",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Fuel Efficiency", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table4_fact_fuel2.py"]}}]
#     )

#     # ========================================================
#     # REPORTING AND FINALIZATION
#     # ========================================================

#     monthly_branch_task = BranchPythonOperator(task_id="monthly_check", python_callable=monthly_check)

#     # Monthly Payroll and Safety Report [cite: 38, 101]
#     monthly_report_task = EmrAddStepsOperator(
#         task_id="monthly_report",
#         job_flow_id=EMR_CLUSTER_ID,
#         steps=[{"Name": "Monthly Report", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": SPARK_ARGS + ["s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/reportgenrate.py"]}}]
#     )

#     skip_monthly_task = EmptyOperator(task_id="skip_monthly")

#     # Final anchor to mark the DAG as successful
#     end_task = EmptyOperator(
#         task_id="end", 
#         trigger_rule="none_failed_min_one_success"
#     )

#     # ========================================================
#     # PIPELINE GRAPH DEPENDENCIES
#     # ========================================================

#     wait_for_dag1 >> start_task >> [branch, maintenance_branch]

#     # Flow A: Daily Registry -> Gold Pipeline
#     branch >> run_daily_silver >> vehicle_registry >> vehicle_assignment >> start_gold_pipeline
#     start_gold_pipeline >> violation >> violation_sensor >> driver >> driver_sensor >> [asset, monthly_branch_task]
#     asset >> asset_sensor >> fleet >> fleet_sensor

#     # Flow B: Fuel Silver -> Registry Sensor -> Fuel Gold
#     branch >> run_fuel_silver >> fuel_silver >> wait_for_registry_00_00 >> fuel_gold
#     branch >> skip

#     # Flow C: Maintenance (Yearly)
#     maintenance_branch >> skip_maintenance >> maintenance_join
#     maintenance_branch >> run_maintenance >> maintenance_silver >> maintenance_join
    
#     # Gold Dependencies
#     # wait_for_registry_00_00 is the primary trigger for 07:00 UTC fuel jobs [cite: 191]
#     [wait_for_registry_00_00, maintenance_join] >> fuel_gold

#     # Flow D: Monthly Reporting
#     monthly_branch_task >> [monthly_report_task, skip_monthly_task]

#     # Final Joins
#     [fuel_gold, fleet_sensor, monthly_report_task, skip_monthly_task, skip] >> end_task





from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import logging

# ============================================================
# CONFIGURATION
# ============================================================

# EMR Cluster ID
EMR_CLUSTER_ID = Variable.get("emr_id")

# Email Recipients
EMAIL_LIST = [
    "harsh.vardhan1@tothenew.com",
    "deepak.bhatia@tothenew.com",
    "tanmay.joshi@tothenew.com"
]

# Spark submit arguments
SPARK_ARGS = [
    "spark-submit",
    "--deploy-mode", "cluster",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:42.7.3"
]

# ============================================================
# EMAIL FAILURE CALLBACK
# ============================================================

def send_failure_email(context):
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    subject = f"🚨 Airflow Task Failed: {dag_run.dag_id}"

    body = f"""
    <h3>DAG Failure Alert</h3>
    
    <p><b>DAG Name:</b> {dag_run.dag_id}</p>
    <p><b>Task Name:</b> {task_instance.task_id}</p>
    <p><b>Execution Time:</b> {context.get('execution_date')}</p>
    <p><b>Log URL:</b> {task_instance.log_url}</p>
    <p><b>Error:</b> {exception}</p>
    """

    send_email(
        to=EMAIL_LIST,
        subject=subject,
        html_content=body
    )

default_args = {
    "owner": "deepak",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_email
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def start_logging():
    logging.info("Batch Pipeline Started")


def choose_flow(**context):
    """Branches DAG based on execution hour"""
    hour = context["execution_date"].hour

    if hour == 0:
        return "run_daily_silver"
    elif hour == 5:
        return "run_fuel_silver"

    return "skip"


def maintenance_check(**context):
    """Run maintenance only on Jan 1"""
    dt = context["execution_date"]

    if dt.month == 1 and dt.day == 1:
        return "run_maintenance"

    return "skip_maintenance"


def monthly_check(**context):
    """Run monthly report on 1st day of month"""
    if context["execution_date"].day == 1:
        return "monthly_report"

    return "skip_monthly"


def get_latest_dag1_execution(execution_date, **context):
    minute = (execution_date.minute // 10) * 10

    rounded_time = execution_date.replace(
        minute=minute,
        second=0,
        microsecond=0
    )

    # Previous completed run
    return rounded_time - timedelta(minutes=10)


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="dag2_batch_pipeline_harsh",
    start_date=datetime(2026, 4, 29),
    schedule="0 0,5 * * *",
    catchup=False,
    default_args=default_args
) as dag:

    # ========================================================
    # WAIT FOR DAG1
    # ========================================================

    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1",
        external_dag_id="dag1_streaming_monitoring_harshTest",
        external_task_id="check_emr_health",
        allowed_states=["success"],
        execution_date_fn=get_latest_dag1_execution,
        poke_interval=30,
        timeout=600,
        mode="reschedule"
    )

    start_task = PythonOperator(
        task_id="start",
        python_callable=start_logging
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_flow
    )

    skip = EmptyOperator(task_id="skip")

    # ========================================================
    # SILVER LAYER
    # ========================================================

    run_daily_silver = EmptyOperator(
        task_id="run_daily_silver"
    )

    vehicle_registry = GlueJobOperator(
        task_id="vehicle_registry_silver",
        job_name="dim_vehicle",
        region_name="us-east-1"
    )

    vehicle_assignment = GlueJobOperator(
        task_id="vehicle_assignment_silver",
        job_name="assignment_grp4_clean",
        region_name="us-east-1"
    )

    run_fuel_silver = EmptyOperator(
        task_id="run_fuel_silver"
    )

    fuel_silver = GlueJobOperator(
        task_id="fuel_transactions_silver",
        job_name="fuel_transactions_clean",
        region_name="us-east-1"
    )

    wait_for_registry_00_00 = ExternalTaskSensor(
        task_id="wait_for_registry_00_00",
        external_dag_id="dag2_batch_pipeline_harsh",
        external_task_id="vehicle_registry_silver",
        execution_delta=timedelta(hours=5),
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule"
    )

    # ========================================================
    # MAINTENANCE FLOW
    # ========================================================

    maintenance_branch = BranchPythonOperator(
        task_id="maintenance_check",
        python_callable=maintenance_check
    )

    run_maintenance = EmptyOperator(
        task_id="run_maintenance"
    )

    maintenance_silver = GlueJobOperator(
        task_id="maintenance_silver",
        job_name="dim_maintenance",
        region_name="us-east-1"
    )

    skip_maintenance = EmptyOperator(
        task_id="skip_maintenance"
    )

    maintenance_join = EmptyOperator(
        task_id="maintenance_join",
        trigger_rule="none_failed_min_one_success"
    )

    # ========================================================
    # GOLD LAYER
    # ========================================================

    start_gold_pipeline = EmptyOperator(
        task_id="start_gold_pipe_line"
    )

    violation = EmrAddStepsOperator(
        task_id="violation_events_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Violation Events",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table1_violation_events.py"
                    ]
                }
            }
        ]
    )

    violation_sensor = EmrStepSensor(
        task_id="wait_violation",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('violation_events_gold')[0] }}"
    )

    driver = EmrAddStepsOperator(
        task_id="driver_safety_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Driver Safety",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/driver_safety_status_4.py"
                    ]
                }
            }
        ]
    )

    driver_sensor = EmrStepSensor(
        task_id="wait_driver",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('driver_safety_gold')[0] }}"
    )

    asset = EmrAddStepsOperator(
        task_id="asset_history_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Asset History",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/driver_history2.py"
                    ]
                }
            }
        ]
    )

    asset_sensor = EmrStepSensor(
        task_id="wait_asset",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('asset_history_gold')[0] }}"
    )

    fleet = EmrAddStepsOperator(
        task_id="fleet_snapshot_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Fleet Snapshot",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/active_fleet.py"
                    ]
                }
            }
        ]
    )

    fleet_sensor = EmrStepSensor(
        task_id="wait_fleet",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('fleet_snapshot_gold')[0] }}"
    )

    fuel_gold = EmrAddStepsOperator(
        task_id="fuel_efficiency_gold",
        trigger_rule="none_failed_min_one_success",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Fuel Efficiency",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/table4_fact_fuel2.py"
                    ]
                }
            }
        ]
    )

    # ========================================================
    # MONTHLY REPORT
    # ========================================================

    monthly_branch_task = BranchPythonOperator(
        task_id="monthly_check",
        python_callable=monthly_check
    )

    monthly_report_task = EmrAddStepsOperator(
        task_id="monthly_report",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "Monthly Report",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": SPARK_ARGS + [
                        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/scripts/reportgenrate.py"
                    ]
                }
            }
        ]
    )

    skip_monthly_task = EmptyOperator(
        task_id="skip_monthly"
    )

    # ========================================================
    # END TASK
    # ========================================================

    end_task = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    wait_for_dag1 >> start_task >> [branch, maintenance_branch]

    # Daily flow
    branch >> run_daily_silver >> vehicle_registry >> vehicle_assignment >> start_gold_pipeline
    start_gold_pipeline >> violation >> violation_sensor >> driver >> driver_sensor
    driver_sensor >> [asset, monthly_branch_task]

    asset >> asset_sensor >> fleet >> fleet_sensor

    # Fuel flow
    branch >> run_fuel_silver >> fuel_silver >> wait_for_registry_00_00 >> fuel_gold
    branch >> skip

    # Maintenance flow
    maintenance_branch >> skip_maintenance >> maintenance_join
    maintenance_branch >> run_maintenance >> maintenance_silver >> maintenance_join

    # Gold dependency
    [wait_for_registry_00_00, maintenance_join] >> fuel_gold

    # Monthly reporting
    monthly_branch_task >> [monthly_report_task, skip_monthly_task]

    # Final join
    [fuel_gold, fleet_sensor, monthly_report_task, skip_monthly_task, skip] >> end_task