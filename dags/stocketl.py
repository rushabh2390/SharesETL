from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='stock_etl',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    install_deps = BashOperator(
        task_id='install_deps',
        cwd='/opt/airflow/dags/stocketl',
        bash_command='pip install -r requirements.txt'
    )

    make_connection = BashOperator(
        task_id='make_connection',
        cwd='/opt/airflow/dags/stocketl',
        bash_command= 'python database.py'
    )

    create_table = BashOperator(
        task_id='create_table',
        cwd='/opt/airflow/dags/stocketl',
        bash_command= 'python tablecreation.py'
    )

    call_scraper = BashOperator(
        task_id='call_scraper',
        cwd='/opt/airflow/dags/stocketl',
        bash_command= 'python scrapehistoricaldata.py'
    )

    data_insertion = BashOperator(
        task_id='data_insertion',
        cwd='/opt/airflow/dags/stocketl',
        bash_command= 'python historicaldatainsertion.py'
    )

    # call_minio = BashOperator(
    #     task_id='call_minio',
    #     cwd='/opt/airflow/dags/stocketl',
    #     # bash_command='cd $(mktemp -d) && curl --output mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && ./mc alias set stocketl http://nginx:9000 minioadmin minioadmin && ls /opt/airflow/dags/stocketl | awk "{print $9}" | grep csv | awk "(NR>1)" | xargs ./mc cp /opt/airflow/dags/stocketl/$1 stocketl/stocketl'
    #     bash_command='cd $(mktemp -d) && curl --output mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && ./mc alias set stocketl http://nginx:9000 minioadmin minioadmin && ls /opt/airflow/dags/stocketl | awk "{print $9}" | grep csv | xargs -I ! ./mc cp /opt/airflow/dags/stocketl/! stocketl/stocketl'
    # )

    # see_csv_files = BashOperator(
    #     task_id='see_csv_files',
    #     cwd='/opt/airflow/dags/stocketl',
    #     bash_command='ls | grep csv | xargs cat'
    #     # bash_command='cat /opt/airflow/dags/stocketl/ABB.csv'
    # )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> install_deps
install_deps >> make_connection
make_connection >> create_table
create_table >> call_scraper
call_scraper >> data_insertion
data_insertion >> end_task
