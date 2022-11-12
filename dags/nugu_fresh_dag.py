from datetime import timedelta,datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import nugu_fresh

nugu_fresh_conn_id = 'nugu_fresh_conn_id'

default_args={
    'owner': 'nugu_fresh',
    'depends_on_past':False,
    'start_date':datetime(2022,8,25),
    'catchup': False, 
    'retries':1, 
    'retry_delay': timedelta(minutes=5),
} 

dag = DAG('nugu_fresh_elt', default_args=default_args, schedule_interval=None)

extract_load_price_input = BranchPythonOperator(
    task_id ='extract_load_price_input',
    python_callable=nugu_fresh.extract_load_price_input,
    op_kwargs={
        'mysql_conn_id' : nugu_fresh_conn_id,
        'execution_date': "{{ ( execution_date ).strftime('%Y-%m-%d') }}",
    },
    provide_context=True,
    dag= dag
)

extract_load_other_input = PythonOperator(
    task_id ='extract_load_other_input',
    python_callable=nugu_fresh.extract_load_other_input,
    op_kwargs={
            'mysql_conn_id' : nugu_fresh_conn_id,
            'execution_date' : "{{ ( execution_date ).strftime('%Y-%m-%d') }}",
        },
    provide_context=True,
    dag= dag
)

transform_load_price_output = BranchPythonOperator(
    task_id ='transform_load_price_output',
    python_callable = nugu_fresh.transform_load_price_output,
    op_kwargs={
            'mysql_conn_id' : nugu_fresh_conn_id,
            'execution_date': "{{ ( execution_date ).strftime('%Y-%m-%d') }}",
        },
    provide_context=True,
    dag= dag
)

transform_price_output = PythonOperator(
    task_id ='transform_price_output',
    trigger_rule='none_failed_or_skipped',
    python_callable = nugu_fresh.transform_price_output,
    op_kwargs={
            'mysql_conn_id' : nugu_fresh_conn_id,
            'execution_date': "{{ ( execution_date ).strftime('%Y-%m-%d') }}",
        },
    provide_context=True,
    dag= dag
)

completion_nugu_fresh_elt = DummyOperator(
    task_id='completion_nugu_fresh_elt',
    trigger_rule = 'none_failed_or_skipped',
    dag=dag,
)

# 없는 가격도 존재하고, 있는 가격도 존재하는 경우
extract_load_price_input >> extract_load_other_input >> transform_load_price_output >> transform_price_output >> completion_nugu_fresh_elt
# 가격이 모두 존재하는 경우
extract_load_price_input >> extract_load_other_input >> transform_load_price_output >> completion_nugu_fresh_elt
# 가격이 모두 없는 경우
extract_load_price_input >> transform_price_output >> completion_nugu_fresh_elt
