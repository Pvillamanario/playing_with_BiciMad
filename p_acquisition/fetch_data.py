import os
from dotenv import load_dotenv
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import requests
import pandas as pd

# Project datasets:

ayto_datasets_base_url = 'https://datos.madrid.es/egob'

datasets_dict = {
    'Aparcamientos públicos municipales'                                        :'/catalogo/202625-0-aparcamientos-publicos.json',
    'Aparcamientos públicos municipales disuasorios'                            :'/catalogo/300531-0-aparcamientos-publicos.json',
    'Aparcamientos municipales para residentes (PAR)'                           :'/catalogo/202584-0-aparcamientos-residentes.json',
    'Monumentos de la ciudad de Madrid'                                         :'/catalogo/300356-0-monumentos-ciudad-madrid.json',
    'Sedes. Centros de Atención Médica'                                         :'/catalogo/212769-0-atencion-medica.json',
    'Sedes. Centros con Espacios Deportivos'                                    :'/catalogo/212808-0-espacio-deporte.json',
    'Templos e iglesias católicas'                                              :'/catalogo/209426-0-templos-catolicas.json',
    'Templos e iglesias no católicas'                                           :'/catalogo/209434-0-templos-otros.json',
    'Edificios de carácter monumental'                                          :'/catalogo/208844-0-monumentos-edificios.json',
    'Embajadas y Consulados'                                                    :'/catalogo/201000-0-embajadas-consulados.json',
    'Museos de la ciudad de Madrid'                                             :'/catalogo/201132-0-museos.json',
    'Deportes. Instalaciones Deportivas Básicas Municipales'                    :'/catalogo/200215-0-instalaciones-deportivas.json',
    'Principales parques y jardines municipales'                                :'/catalogo/200761-0-parques-jardines.json',
    'Centros Culturales Municipales (incluyen Socioculturales y Juveniles)'     :'/catalogo/200304-0-centros-culturales.json',
    'Escuelas Infantiles Municipales'                                           :'/catalogo/202318-0-escuelas-infantiles.json',
    'Colegios Públicos de Madrid'                                               :'/catalogo/202311-0-colegios-publicos.json'
}


def fetch_azure_dataset():

    print('Fetching data from Azure SQL')

    # Retrieve credentials
    load_dotenv('.env')
    AZ_user = os.environ.get('AZURE_DB_USER')
    AZ_pass = os.environ.get('AZURE_DB_PASS')

    # Azure DB connection params
    driver = 'Driver={ODBC Driver 17 for SQL Server};'
    server = 'Server=tcp:sqlironhack.database.windows.net,1433;'
    database = 'Database=BiciMAD;'
    uid = f'Uid={AZ_user};'
    pwd = f'Pwd={AZ_pass};'
    config = 'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # connection string assembly
    connection_string = driver+server+database+uid+pwd+config 
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    # SQL Engine 
    engine = create_engine(connection_url)
    print('SQL engine created')

    # Getting data
    df_BiciMad = pd.read_sql_query("SELECT * FROM dbo.bicimad_stations", engine)
    df_BiciMad.to_csv('.\data\BiciMad.csv', index=False)

    print(f'\n {df_BiciMad.shape[0]} stations found')
    print('\n Data collected and saved\n')

def retrieve_datasets_ayto(data_dic):
    
    df_datasets_ayto = pd.DataFrame(columns=['dataset', 'name', 'address', 'zip_code', 'longitude', 'latitude'])

    print('Fetching data from Ayto. Madrid')

    for dataset in list(data_dic.keys()):

        print(f'Loading data for {dataset}')


        dataset_url = ayto_datasets_base_url + data_dic[dataset]
            
        response = requests.get(dataset_url)

        json_data = response.json()

        for i in range(len(json_data['@graph'])):
            try:
                row ={
                    'dataset': dataset,
                    'name': json_data['@graph'][i]['title'],
                    'address': json_data['@graph'][i]['address']['street-address'],
                    'zip_code': json_data['@graph'][i]['address']['postal-code'],
                    'longitude': json_data['@graph'][i]['location']['longitude'],
                    'latitude': json_data['@graph'][i]['location']['latitude'] 
                }
            except:
                row ={
                    'dataset': dataset,
                    'name': json_data['@graph'][i]['title'],
                    'address': 'ERROR',
                    'zip_code': 'ERROR',
                    'longitude': 'ERROR',
                    'latitude': 'ERROR' 
                }

            df_datasets_ayto = df_datasets_ayto.append(row, ignore_index=True)
        
    df_datasets_ayto.to_csv('.\data\datasets_ayto.csv', index=False)
    
    print(f'\n{df_datasets_ayto.shape[0]} points of interest found')
    print('\nData collected and saved\n')

fetch_azure_dataset()
retrieve_datasets_ayto(datasets_dict)
