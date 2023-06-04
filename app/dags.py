from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

from app.constants import PATH_ROOT

'''
IMPORTANT: because of some problems in airflow installation, I couldn't test this orchestration enough
As an alternative, run main.py
'''

default_args = {
   'owner': 'matheus_candido',
   'depends_on_past': False,
   'start_date': datetime(2023, 6, 4),
   'retries': 0,
}

with DAG(
        'breweries_extraction',
        schedule=timedelta(days=1),
        catchup=False,
        default_args=default_args
) as dag:
    
    test_brewery_api = BashOperator(
        task_id='test_brewery_api',
        bash_command='python3 {}app/tests/extractor/test_brewery_api.py'.format(PATH_ROOT)
    )

    extract_data_api = BashOperator(
        task_id='extract_data_api',
        bash_command='python3 {}app/extractor/extract_data_api.py'.format(PATH_ROOT)
    )

    source_to_delta = BashOperator(
        task_id='source_to_delta',
        bash_command='python3 {}app/layers/bronze/source_to_delta.py'.format(PATH_ROOT)
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python3 {}app/layers/bronze/bronze_to_silver.py'.format(PATH_ROOT)
    )

    test_bronze_to_silver = BashOperator(
        task_id='test_bronze_to_silver',
        bash_command='python3 {}app/tests/layes/bronze/test_bronze_to_silver.py'.format(PATH_ROOT)
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='python3 {}app/layers/silver/silver_to_gold.py'.format(PATH_ROOT)
    )

    test_silver_to_gold = BashOperator(
        task_id='test_silver_to_gold',
        bash_command='python3 {}app/tests/layes/silver/test_silver_to_gold.py'.format(PATH_ROOT)
    )

    ''' DEBUG ONLY
    display_gold_data = BashOperator(
        task_id='display_gold_data',
        bash_command='python3 {}app/layers/gold/display_gold_data.py'.format(PATH_ROOT)
    )
    '''

    test_brewery_api >> extract_data_api
    extract_data_api >> source_to_delta
    source_to_delta >> bronze_to_silver
    bronze_to_silver >> test_bronze_to_silver
    test_bronze_to_silver >> silver_to_gold
    silver_to_gold >> test_silver_to_gold