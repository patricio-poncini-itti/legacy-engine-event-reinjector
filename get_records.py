import boto3
import csv
import json
import decimal

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime, timezone

CSV_INPUT_FILE = 'iks_example.csv'  # Nombre de tu archivo CSV de entrada. Se pone el de test por defecto
CSV_OUTPUT_FILE = 'output_data.csv'  # Nombre para el archivo CSV de salida
DYNAMODB_TABLE_NAME = 'your-dynamo-table'  # El nombre de tu tabla en DynamoDB
PRIMARY_KEY_NAME = 'PK'  # El nombre del atributo de tu clave primaria
CSV_PK_COLUMN_NAME = 'Idempotency Key'  # El nombre de la columna en tu CSV que contiene las PKs
AWS_REGION = 'your-aws-region'

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

class DecimalEncoder(json.JSONEncoder):
    """
    Esta clase ayuda a convertir los objetos Decimal de DynamoDB
    en enteros o flotantes que JSON sí entiende.
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)


def transformar_item(item_original):
    """
    Toma un item de DynamoDB y lo convierte al nuevo formato deseado.
    """
    pk_value = item_original.get('PK')

    campos_principales_conocidos = [
        'PK', 'SK', 'reported_date', 'business_unit', 'member_id',
        'event_type', 'amount', 'is_refund', 'created_at', 'entity_type',
        'transaction_id', 'transaction_code', 'currency'
    ]

    data_object = {
        "reportedDate": item_original.get('reported_date'),
        "businessUnitId": str(item_original.get('business_unit')),  # Convertido a string
        "memberId": item_original.get('member_id'),
        "eventTypeId": item_original.get('event_type'),
        "amount": item_original.get('amount'),
        "isRefund": item_original.get('is_refund'),
        "batch": pk_value,
    }

    nuevo_formato = {
        "eventType": "EVENT_CREATED",  # Valor fijo
        "dateTime": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),  # Fecha y hora actual
        "version": "v1.0",  # Valor fijo
        "idempotencyKey": pk_value,
        "data": [data_object]  # El objeto 'data' va dentro de una lista
    }

    return nuevo_formato


def query_items_by_pk(pk_value):
    all_items = []
    try:
        response = table.query(KeyConditionExpression=Key(PRIMARY_KEY_NAME).eq(pk_value))
        all_items.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key(PRIMARY_KEY_NAME).eq(pk_value),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            all_items.extend(response.get('Items', []))
    except ClientError as e:
        print(f"Error de AWS al consultar la PK {pk_value}: {e.response['Error']['Message']}")
        return None
    return all_items


def main():
    print(f"Leyendo claves desde '{CSV_INPUT_FILE}'...")
    pks_to_fetch = []
    try:
        with open(CSV_INPUT_FILE, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                pks_to_fetch.append(row[CSV_PK_COLUMN_NAME])
        unique_pks = list(set(pks_to_fetch))
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de entrada '{CSV_INPUT_FILE}'.")
        return

    print(f"Se encontraron {len(unique_pks)} claves únicas para consultar...")

    with open(CSV_OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Searched_PK', 'Found_Item_Body'])

        for i, pk_value in enumerate(unique_pks):
            print(f"Consultando registros para la PK {i + 1}/{len(unique_pks)}: {pk_value}")
            items = query_items_by_pk(pk_value)

            if items is None:
                writer.writerow([pk_value, 'ERROR_DURANTE_LA_CONSULTA'])
                continue

            if items:
                for item in items:
                    item_transformado = transformar_item(item)
                    json_body = json.dumps(item_transformado, cls=DecimalEncoder, indent=4, ensure_ascii=False)
                    writer.writerow([pk_value, json_body])
            else:
                writer.writerow([pk_value, 'NO_ENCONTRADO'])

    print("¡Proceso completado exitosamente! ✨")


if __name__ == '__main__':
    main()