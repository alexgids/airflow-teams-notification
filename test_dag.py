# -*- coding: utf-8 -*-
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from .teams_notify import TeamsNotification

default_args = {
	"owner"           		: "Airflow",
	"start_date"	  		: days_ago(2),
	"retries"         		: 2,
	"retry_delay"     		: datetime.timedelta( minutes= 5 ),
	"provide_context" 		: True
}


with airflow.DAG( "teams_airflow_test", default_args= default_args, schedule_interval=None, catchup=False,on_success_callback=TeamsNotification("teams_webhook").success_alert,on_failure_callback=TeamsNotification("teams_webhook").error_alert) as dag:

	start = DummyOperator( task_id= "start" )

    fail_task_test = BashOperator(
    task_id='fail_task_test',
    bash_command='exit 1',
    dag=dag)

    success_task_test = BashOperator(
    task_id='success_task_test',
    bash_command='exit 0',
    dag=dag)

	stop  = DummyOperator( task_id= "stop"  )



start >> fail_task_test >> stop
start >> success_task_test >> stop