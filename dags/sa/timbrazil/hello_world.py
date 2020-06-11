"""Anonymize raw sms data files from TIM Brazil.
Frequency: Hourly incremental extraction for the previous hour.
Inputs:
    S3: timbrazil-internal/ocs/ocs_sms/y/m/d/h/
Outputs:
    S3: timbrazil-public/ocs/ocs_sms/y/m/d/h/
Alerts:
    airflow@juvo.com
"""
from datetime import timedelta

from airflow import DAG

from dag.operators.ingestion import PIIOperator
from dag.settings import timbrazil
from dag.tasks.timbrazil import anonymization

default_args = {
    'owner': 'aaron.mangum',
    'depends_on_past': False,
    'email': ['airflow@juvo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'sla': timedelta(hours=24),
    'start_date': timbrazil.LAUNCH_DATE,
}

dag = DAG(
    'timbrazil.anonymize_sms', schedule_interval='@hourly', default_args=default_args
)

PATH = 's3://{}/ocs/ocs_sms'
MSISDN_COLUMN = 22
IMEI_COLUMN = 390
PII_COLUMNS = [22, 32, 372, 373, 374, 375, 376, 377, 390, 396]

ingest = PIIOperator(
    task_id='ingest',
    input_path=PATH.format(timbrazil.RAW_CARRIER_BUCKET),
    output_path=PATH.format(timbrazil.ANONYMIZED_CARRIER_BUCKET),
    uuid_read_path=timbrazil.UUID_READ_PATH,
    uuid_write_path=timbrazil.UUID_WRITE_PATH,
    anonymizer_conn_id=timbrazil.ANONYMIZER_CONN_ID,
    msisdn_column=MSISDN_COLUMN,
    pii_columns=PII_COLUMNS,
    transform_func=anonymization.transformer_factory(MSISDN_COLUMN, IMEI_COLUMN),
    csv_kwargs=anonymization.get_csv_kwargs('gzip'),
    dag=dag,
)
