from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas
import pymongo

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['pabloalfaro@correo.ugr.es'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='01practica',
    default_args = default_args,
    description = 'Tareas de Airflow',
    dagrun_timeout=timedelta(minutes=80),
    schedule_interval=timedelta(days=1),
)


# Creo una carpeta
NuevoDirectorio = BashOperator(
    task_id='NuevoDirectorio',
    depends_on_past=False,
    bash_command='mkdir -p /tmp/practica2/',
    dag=dag
)

# Descargo el fichero con los datos de temperatura
DatosTemperatura = BashOperator(
    task_id='DatosTemperatura',
    depends_on_past=False,
    bash_command='curl -o /tmp/practica2/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag
)

# Descargo el fichero con los datos de humedad
DatosHumedad = BashOperator(
    task_id='DatosHumedad',
    depends_on_past=False,
    bash_command='curl -o /tmp/practica2/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag
)

# Descomprimo los datos de temperatura
def descomprimirTemperatura():
    from zipfile import ZipFile
    with ZipFile('/tmp/practica2/temperature.csv.zip', 'r') as zip:
        zip.extractall('/tmp/practica2/')
        
DescomprimirTemperatura = PythonOperator(
    task_id='DescomprimirTemperatura',
    python_callable=descomprimirTemperatura,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Descomprimo los datos de humedad
def descomprimirHumedad():
    from zipfile import ZipFile
    with ZipFile('/tmp/practica2/humidity.csv.zip', 'r') as zip:
        zip.extractall('/tmp/practica2/')
        
DescomprimirHumedad = PythonOperator(
    task_id='DescomprimirHumedad',
    python_callable=descomprimirHumedad,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tratamiento de datos
def preprocesado():
    datos_temp = pandas.read_csv('/tmp/practica2/temperature.csv')
    datos_hume = pandas.read_csv('/tmp/practica2/humidity.csv')
    datos_sf_temp = datos_temp['San Francisco']
    datos_sf_hume = datos_hume['San Francisco']
    datos_fecha = datos_hume['datetime']
    columnas = {'DATE': datos_fecha, 'TEMP': datos_sf_temp, 'HUM': datos_sf_hume}
    datos_tratados = pandas.DataFrame(data=columnas)
    datos_tratados.fillna(datos_tratados.mean())
    datos_tratados.to_csv('/tmp/practica2/san_francisco.csv', encoding='utf-8', sep='\t', index=False)

Preprocesado = PythonOperator(
    task_id='Preprocesado',
    python_callable=preprocesado,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Inserción de datos
def insercion():
    datos_tratados = pandas.read_csv('/tmp/practica2/san_francisco.csv', sep='\t')
    diccionario = datos_tratados.to_dict('records')
    client = pymongo.MongoClient("mongodb+srv://cc-user:P8jVmeMpzSLfYIvY@cluster0.pupdc.mongodb.net/ccairflow?retryWrites=true&w=majority")
    code = client.ccairflow['sanfrancisco'].insert_many(diccionario)

Insercion = PythonOperator(
    task_id='Insercion',
    python_callable=insercion,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Descarga de la versión 1 de la api
DescargarApi1 = BashOperator(
    task_id='DescargarApi1',
    depends_on_past=False,
    bash_command='rm -f /tmp/practica2/cc2_p2_api1.zip; curl -o /tmp/practica2/cc2_p2_api1.zip -LJ https://github.com/pabloalfaro/cc2_p2_api1/archive/refs/heads/main.zip',
    dag=dag
)

# Descarga de la versión 2 de la api
DescargarApi2 = BashOperator(
    task_id='DescargarApi2',
    depends_on_past=False,
    bash_command='rm -f /tmp/practica2/cc2_p2_api2.zip; curl -o /tmp/practica2/cc2_p2_api2.zip -LJ https://github.com/pabloalfaro/cc2_p2_api2/archive/refs/heads/main.zip',
    dag=dag
)

# Descomprimo los datos de la primera versión de la api
def descomprimirApi1():
    from zipfile import ZipFile
    with ZipFile('/tmp/practica2/cc2_p2_api1.zip', 'r') as zip:
        zip.extractall('/tmp/practica2/')
        
DescomprimirApi1 = PythonOperator(
    task_id='DescomprimirApi1',
    python_callable=descomprimirApi1,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Descomprimo los datos de la segunda versión de la api
def descomprimirApi2():
    from zipfile import ZipFile
    with ZipFile('/tmp/practica2/cc2_p2_api2.zip', 'r') as zip:
        zip.extractall('/tmp/practica2/')
        
DescomprimirApi2 = PythonOperator(
    task_id='DescomprimirApi2',
    python_callable=descomprimirApi2,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Test api 1
TestApi1 = BashOperator(
    task_id='TestApi1',
    depends_on_past=False,
    bash_command='cd /tmp/practica2/cc2_p2_api1-main/ && python -m pytest test.py',
    dag=dag
)

# Test api 2
TestApi2 = BashOperator(
    task_id='TestApi2',
    depends_on_past=False,
    bash_command='cd /tmp/practica2/cc2_p2_api2-main/ && python -m pytest test.py',
    dag=dag
)

NuevoDirectorio >> [DatosTemperatura, DatosHumedad] >> DescomprimirTemperatura
DescomprimirTemperatura >> DescomprimirHumedad >> Preprocesado
Preprocesado >> Insercion >> [DescargarApi1, DescargarApi2] 
DescargarApi1 >> DescomprimirApi1 >> TestApi1
DescargarApi2 >> DescomprimirApi2 >> TestApi2

