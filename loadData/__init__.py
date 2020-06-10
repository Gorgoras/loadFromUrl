import logging
import os
import json
import pandas
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
from io import BytesIO
from datetime import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential
from azure.identity import ClientSecretCredential
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import BlobServiceClient
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Empezando proceso de carga a partir de urls.')

    # nombre de la tabla para buscar las urls
    name = "urlToLake"

    # traer connection string como secreto del key vault
    retrieved_secret = getConnectionString()

    # creamos el objeto para conectarnos al table storage
    table_service = TableService(connection_string=retrieved_secret.value)

    # armo dataframe a partir de una tabla
    df = get_dataframe_from_table_storage_table(table_service, name)

    try:
        # recorremos la tabla
        for i, o in df.iterrows():
            logging.info("Loop nro: {}".format(i + 1))

            # capturo nombre de directorio de la tabla
            lake_dir = o['lakeDirectory']

            # traer data desde url a un dataframe
            get = requests.get(o['url'])
            df_loop = pandas.read_csv(
                StringIO(get.content.decode('windows-1252')))

            # timestamp para nombre del archivo
            timestamp = datetime.now().strftime("%d-%b-%YT%H-%M-%S")

            # cliente para data lake
            blob_service_client = BlobServiceClient.from_connection_string(
                retrieved_secret.value)

            # referencia al archivo y creacion de blob en nube
            nombre_archivo = "{0}/{0}_{1}.parquet".format(lake_dir, timestamp)
            blob_client = blob_service_client.get_blob_client(
                container='raw-tests', blob=nombre_archivo)

            # carga de dataframe al blob recien creado
            subirDataframe(df_loop, blob_client)

        ret = dict()
        ret['result'] = "Success"
        return func.HttpResponse(
            json.dumps(ret),
            status_code=200
        )
    except Exception as ex:
        ret = dict()
        ret['result'] = ex
        return func.HttpResponse(
            json.dumps(ret),
            status_code=400
        )


def getConnectionString():
    """Retrieves the connection string using either Managed Identity
    or Service Principal"""

    KeyVault_DNS = os.environ["KeyVault_DNS"]
    SecretName = os.environ["SecretName"]

    try:
        creds = ManagedIdentityCredential()
        client = SecretClient(vault_url=KeyVault_DNS, credential=creds)
        retrieved_secret = client.get_secret(SecretName)
    except BaseException:
        creds = ClientSecretCredential(client_id=os.environ["SP_ID"],
                                       client_secret=os.environ["SP_SECRET"],
                                       tenant_id=os.environ["TENANT_ID"])
        client = SecretClient(vault_url=KeyVault_DNS, credential=creds)
        retrieved_secret = client.get_secret(SecretName)

    return retrieved_secret


def get_data_from_table_storage_table(table_service, table_name):
    """ Retrieve data from Table Storage """

    SOURCE_TABLE = table_name
    for record in table_service.query_entities(
        SOURCE_TABLE
    ):
        yield record


def get_dataframe_from_table_storage_table(table_service, table_name):
    """ Create a dataframe from table storage data """

    return pandas.DataFrame(
        get_data_from_table_storage_table(table_service, table_name))


def subirDataframe(df_to_upload, blob_client):
    """Sube un dataframe al archivo pasado como parametro"""

    # escribe tabla en un buffer de memoria
    table = pa.Table.from_pandas(df_to_upload)
    buf = BytesIO()
    pq.write_table(table, buf)

    # subida de informacion
    logging.info('Subiendo informacion al lake')
    with buf as data:
        blob_client.upload_blob(data)
