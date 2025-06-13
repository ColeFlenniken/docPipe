from langchain_community.document_loaders import PyPDFLoader
from langchain_community.document_loaders import AzureBlobStorageFileLoader
from langchain.schema import Document
import asyncio
from azure.storage.blob import BlobClient
import tempfile
import os





def ingest_Azure_Blob(blob_name : str, container_name : str, conn_str : str, additional_metadata : dict[str,str] | None = None ) -> Document:
    blob_client = BlobClient.from_blob_url(file_path)
    blob_props = blob_client.get_blob_properties()
    blob_metadata = dict(blob_props.metadata) if blob_props.metadata else {}

    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(blob_client.blob_name)[1]) as tmp_file:
        download_stream = blob_client.download_blob()
        tmp_file.write(download_stream.readall())
        tmp_file_path = tmp_file.name

    doc = AzureBlobStorageFileLoader(conn_str=conn_str, container=container_name, conn_str=conn_str).load()
    
    for k, v in blob_metadata.items():
        doc.metadata[k] = v
    for k,v in additional_metadata:
        doc.metadata[k] = v

    doc.metadata['blob_name'] = blob_client.blob_name
    doc.metadata['container'] = blob_client.container_name
    doc.metadata['size'] = blob_props.size
    os.remove(tmp_file_path)
    return doc


    

        
            
    

        
asyncio.run(ingest_PDF("C:\\Users\\f8col\\OneDrive\\Desktop\\Projects\\docPipe\\HierarchicalRAG.pdf"))