from langchain_community.document_loaders import PyPDFLoader
from langchain_community.document_loaders import AzureBlobStorageFileLoader
from langchain.schema import Document
from azure.storage.blob import BlobClient, BlobServiceClient
import tempfile
import os



def get_new_Azure_blobs(connection_string , container_name) -> list[Document]:
    documents = []    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    for blob in container_client.list_blobs():
        metadata = blob.metadata or {}
        if 'IsIndexed' not in metadata:
            documents.extend(load_Azure_Blob(blob.name, container_name, connection_string))
            print("added " + blob.name)
    return documents


def load_Azure_Blob(blob_name : str, container_name : str, conn_str : str, additional_metadata : dict[str,str] | None = None ) -> Document:
    blob_client = BlobClient.from_connection_string(conn_str, container_name, blob_name)
    blob_props = blob_client.get_blob_properties()
    blob_metadata = dict(blob_props.metadata) if blob_props.metadata else {}

    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(blob_client.blob_name)[1]) as tmp_file:
        download_stream = blob_client.download_blob()
        tmp_file.write(download_stream.readall())
        tmp_file_path = tmp_file.name

    docs = AzureBlobStorageFileLoader(conn_str=conn_str, container=container_name, blob_name=blob_name).load()
    for k, v in blob_metadata.items():
        for item in docs:
            item.metadata[k] = v
    if additional_metadata is not None:
        for k,v in additional_metadata:
            for item in docs:
                item.metadata[k] = v
    for item in docs:
        item.metadata['blob_name'] = blob_client.blob_name
        item.metadata['container'] = blob_client.container_name
        item.metadata['size'] = blob_props.size
    os.remove(tmp_file_path)
    return docs

def chunk_document(doc : Document) -> list[Document]:
    



conn_str = "BlobEndpoint=https://biceptest.blob.core.windows.net/;QueueEndpoint=https://biceptest.queue.core.windows.net/;FileEndpoint=https://biceptest.file.core.windows.net/;TableEndpoint=https://biceptest.table.core.windows.net/;SharedAccessSignature=sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-06-16T05:08:57Z&st=2025-06-14T21:08:57Z&spr=https&sig=0%2BdqfHMXKUpkRa7wga%2FV4Oy7W4UEyv%2BWQY2mbXLaoLY%3D"
container_name = 'original'
