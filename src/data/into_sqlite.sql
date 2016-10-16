CREATE TABLE documents_meta (
    document_id INT,
    source_id INT,
    publisher_id INT,
    publish_time TIMESTAMP
);

.separator ","
.import ../raw/documents_meta.csv documents_meta
