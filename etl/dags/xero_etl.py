from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from libraries.database import create_table, extract_log_data


dag = DAG(
    "log_data_processing",
    start_date=days_ago(1),
    description="A simple DAG to process Xero event log data",
    schedule_interval="@once",
)


t_log = PythonOperator(
    task_id="extract_log_data", python_callable=extract_log_data, dag=dag
)


def etl_table(table_name, table_type, dag):
    return PythonOperator(
        task_id=f"{table_type}_table_{table_name}",
        python_callable=create_table,
        op_kwargs={"table_name": table_name},
        dag=dag,
    )


t_signup = etl_table("signup", "bronze", dag)
t_signup << t_log

t_login = etl_table("login", "bronze", dag)
t_login << t_log

t_plan_change = etl_table("plan_change", "bronze", dag)
t_plan_change << t_log

t_churn_event = etl_table("churn_event", "silver", dag)
t_churn_event << t_login

t_user_location = etl_table("user_location", "silver", dag)
t_user_location << t_signup

t_province_by_year = etl_table("province_by_year", "gold", dag)
t_province_by_year << t_user_location
