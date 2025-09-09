## Script para reinyección de eventos

1. `get_records.py`: En base a un archivo `.csv` busca registros en una tabla específica y los parsea en un nuevo archivo `output_data.csv`
2. `send_csv_file_to_sqs.py`: En base al archivo `output_data.py` itera sobre los registros traídos de la tabla y los envía por SQS utilizando la IK como group id y un uuid random como deduplication id.