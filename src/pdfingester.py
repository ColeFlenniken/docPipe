def ingest_PDF(file_path : str, additional_metadata : dict[str,str]) -> Document:
    loader = PyPDFLoader(file_path=file_path)
    doc : Document = None
    pages_content = []
    for page in loader.load():
        if doc is None:
            doc = page
            doc.metadata.pop('page')
            doc.metadata.pop('page_label')
        pages_content.append(page.page_content)
    doc.page_content = "\n".join(pages_content)
    for k,v in additional_metadata:
        doc.metadata[k] = v
    print(doc.metadata)
    print(len(doc.page_content))
    return doc