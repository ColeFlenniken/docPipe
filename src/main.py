from langchain_community.document_loaders import PyPDFLoader

def ingest(file_name : str) -> list[Document]:
    