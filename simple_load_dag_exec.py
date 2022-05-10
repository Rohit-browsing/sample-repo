# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A simple Airflow DAG that is triggered externally by a Cloud Function when a
file lands in a GCS bucket.
Once triggered the DAG performs the following steps:
1. Triggers a Google Cloud Dataflow job with the input file information received
   from the Cloud Function trigger.
2. Upon completion of the Dataflow job, the input file is moved to a
   gs://<target-bucket>/<success|failure>/YYYY-MM-DD/ location based on the
   status of the previous step.
"""

import datetime
import logging
import os
from datetime import timedelta
from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.email_operator import EmailOperator

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# We define some variables that we will use in the DAG tasks.
SUCCESS_TAG = 'success'
FAILURE_TAG = 'failure'

# An Airflow variable called gcs_completion_bucket is required.
# This variable will contain the name of the bucket to move the processed
# file to.
COMPLETION_BUCKET = models.Variable.get('gcs_completion_bucket')
DS_TAG = '{{ ds }}'
DATAFLOW_FILE = os.path.join(
    configuration.get('core', 'dags_folder'), 'dataflow', 'process_delimited.py')
THRESHOLD_VALUE = int(models.Variable.get('threshold_value'))
# The following additional Airflow variables should be set:
# gcp_project:         Google Cloud Platform project id.
# gcp_temp_location:   Google Cloud Storage location to use for Dataflow temp location.
# email:               Email address to send failure notifications.
DEFAULT_DAG_ARGS = {
	'owner': 'airflow',
    'depends_on_past': True,
    'start_date': YESTERDAY,
    'email': models.Variable.get('email'),
     'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project'),
    'dataflow_default_options': {
        'project': models.Variable.get('gcp_project'),
        'temp_location': models.Variable.get('gcp_temp_location'),
        'runner': 'DataflowRunner'
    }
}

def move_to_completion_bucket(target_bucket, target_infix, **kwargs):
		"""A utility method to move an object to a target location in GCS."""
		# Here we establish a connection hook to GoogleCloudStorage.
		# Google Cloud Composer automatically provides a google_cloud_storage_default
		# connection id that is used by this hook.
		conn = gcs_hook.GoogleCloudStorageHook()

		# The external trigger (Google Cloud Function) that initiates this DAG
		# provides a dag_run.conf dictionary with event attributes that specify
		# the information about the GCS object that triggered this DAG.
		# We extract the bucket and object name from this dictionary.
		source_bucket = kwargs['dag_run'].conf['bucket']
		source_object = kwargs['dag_run'].conf['name']
		completion_ds = kwargs['ds']

		target_object = os.path.join(target_infix, completion_ds, source_object)

		logging.info('Copying %s to %s',
					 os.path.join(source_bucket, source_object),
					 os.path.join(target_bucket, target_object))
		conn.copy(source_bucket, source_object, target_bucket, target_object)

		logging.info('Deleting %s',
					 os.path.join(source_bucket, source_object))
		conn.delete(source_bucket, source_object)

# In BigQueryHook, a google cloud connection is established with permissions related to Big Query and project.
# Here, SQL statement is executed connecting with BigQuery table. It will return the count of records,
# and the value wil be stored in a python variable as int type.
# Further,a comparison is done between BigQuery table count and threshold count.
#If  it matches, it will trigger a success email and same scenario with failure part also.
def get_count_data(**kwargs):
		hook = BigQueryHook(bigquery_conn_id='my_gcp_conn', delegate_to=None, use_legacy_sql=False, location ='us-central1')
		conn = hook.get_conn()
		cursor = conn.cursor()
		cursor.execute("select count(*) from `gcp-poc-338705-344413.dataNavigationDataSet.bq_output_table`")
		result = cursor.fetchall()
		result_num = result[0][0]
		# Displaying the count of records in Big Query table.
		print('Result of table count : ', result_num)
		print('Threshold value is : ',THRESHOLD_VALUE)
		if result_num == THRESHOLD_VALUE:
			return ['match_success_email','success-move-to-completion']
		else:
			return 'match_failed_email'
# Setting schedule_interval to None as this DAG is externally trigger by a Cloud Function.
# The following Airflow variables should be set for this DAG to function:
# bq_output_table: BigQuery table that should be used as the target for
#                  Dataflow in <dataset>.<tablename> format.
#                  e.g. lake.usa_names
# input_field_names: Comma separated field names for the delimited input file.
#                  e.g. state,gender,year,name,number,created_date
with models.DAG(dag_id='GcsToBigQueryTriggered',description='A DAG triggered by an external Cloud Function',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
				
				# Args required for the Dataflow job.
				job_args = {
					'input': 'gs://{{ dag_run.conf["bucket"] }}/{{ dag_run.conf["name"] }}',
					'output': models.Variable.get('bq_output_table'),
					'fields': models.Variable.get('input_field_names'),
					'load_dt': DS_TAG
				}

				# Main Dataflow task that will process and load the input delimited file.
				dataflow_task = dataflow_operator.DataFlowPythonOperator(
					task_id="process-delimited-and-push",
					py_file=DATAFLOW_FILE,
					options=job_args)
				
				#For the record count fetch in a python function
				fetch_data = BranchPythonOperator(
					task_id='count-metrics',
					provide_context=True,
					python_callable=get_count_data
				)
				
				#A success mail will be sent if record count(in BigQuery table) matches with the threshold count
				email_trigger_success = EmailOperator(
						task_id='match_success_email',
						to='ashwarya234@gmail.com',
						subject="Wow!! It's a match!!",
						html_content="""
						Congratulations!!
						The record count is matching with the threshold count.
						"""
						)
				#A failure mail will be sent if record count(in BigQuery table) doesn't match with the threshold count		
				email_trigger_fail = EmailOperator(
						task_id='match_failed_email',
						to='ashwarya234@gmail.com',
						subject="Oops!! It's not a match!!",
						html_content="""
						Sorry!!
						The record count isn't matching with the threshold count.
						"""
						)
					
				# Here we create two conditional tasks, one of which will be executed
				# based on whether the dataflow_task was a success or a failure.
				success_move_task = PythonOperator(task_id='success-move-to-completion',
																   python_callable=move_to_completion_bucket,
																   # A success_tag is used to move
																   # the input file to a success
																   # prefixed folder.
																   op_args=[COMPLETION_BUCKET, SUCCESS_TAG],
																   provide_context=True,
																   trigger_rule=TriggerRule.ALL_SUCCESS)

				# The success_move_task and failure_move_task are both downstream from the
				# dataflow_task.
				dataflow_task >> fetch_data >> email_trigger_success >> success_move_task
				dataflow_task >> fetch_data >> email_trigger_fail