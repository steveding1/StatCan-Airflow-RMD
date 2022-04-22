from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime
import requests
from zipfile import ZipFile

with DAG(
    dag_id="canstat_rmd",
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['mylearn']
) as dag:

    def statcan():
        url = "https://www150.statcan.gc.ca/n1/tbl/csv/10100139-eng.zip"
        #https://www150.statcan.gc.ca/n1/tbl/csv/24100058-eng.zip
        filename = 'vehicles_ent.zip'
        #GET CURRENT REMOTE FILE SIZE
        filesize = requests.head(url).headers['Content-Length']

        def zip(filename):
            with ZipFile(filename, 'r') as zipObj:
                zipObj.extractall()

        def download(filename):
            data = requests.get(url)
            with open(filename, 'wb')as file:
                file.write(data.content)

        # GET LAST FILESIZE FROM THE LOG FILE
        lastsize = 0
        try:
            with open ('filesize.log', 'r') as file:
                lines = file.readlines()
                lastsize = lines[-1].split(',')[-1] if lines else 0
                
        except IOError:
            pass

        if lastsize!=int(filesize)>0:
            #print(lastsize,int(filesize))
            with open ('filesize.log', 'a') as file:
                file.write(
                datetime.now().strftime("%d/%m/%Y %H:%M:%S") +
                ',' + filesize + '\n'
                )
            download(filename)
            zip(filename)
            
    download_csv = PythonVirtualenvOperator(
        task_id = 'download_csv',
        python_callable = statcan,
        requirements=["requests"], 
        system_site_packages=False,
        dag=dag,
    )

