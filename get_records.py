import boto3
import csv
import json
import decimal

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime, timezone

CSV_INPUT_FILE = 'iks_example.csv'  # Name of your input CSV file. Default is the test file.
CSV_OUTPUT_FILE = 'output_data.csv'  # Name for the output CSV file.
DYNAMODB_TABLE_NAME = 'your-dynamo-table'  # The name of your DynamoDB table.
PRIMARY_KEY_NAME = 'PK'  # The name of your primary key attribute.
CSV_PK_COLUMN_NAME = 'Idempotency Key'  # The name of the column in your CSV containing the PKs.
AWS_REGION = 'your-aws-region'

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

class DecimalEncoder(json.JSONEncoder):
    """
    This helper class converts DynamoDB Decimal objects
    into integers or floats that the standard JSON library can understand.
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)


def transform_item(original_item):
    """
    Takes a DynamoDB item and transforms it into the desired new format.
    """
    pk_value = original_item.get('PK')

    known_main_fields = [
        'PK', 'SK', 'reported_date', 'business_unit', 'member_id',
        'event_type', 'amount', 'is_refund', 'created_at', 'entity_type',
        'transaction_id', 'transaction_code', 'currency'
    ]

    data_object = {
        "reportedDate": original_item.get('reported_date'),
        "businessUnitId": str(original_item.get('business_unit')),  # Converted to string
        "memberId": original_item.get('member_id'),
        "eventTypeId": original_item.get('event_type'),
        "amount": original_item.get('amount'),
        "isRefund": original_item.get('is_refund'),
        "batch": pk_value,
    }

    new_format = {
        "eventType": "EVENT_CREATED",  # Fixed value
        "dateTime": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),  # Current date and time
        "version": "v1.0",  # Fixed value
        "idempotencyKey": pk_value,
        "data": [data_object]  # The 'data' object goes inside a list
    }

    return new_format


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
        print(f"AWS error while querying PK {pk_value}: {e.response['Error']['Message']}")
        return None
    return all_items


def main():
    print(f"Reading keys from '{CSV_INPUT_FILE}'...")
    pks_to_fetch = []
    try:
        with open(CSV_INPUT_FILE, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                pks_to_fetch.append(row[CSV_PK_COLUMN_NAME])
        unique_pks = list(set(pks_to_fetch))
    except FileNotFoundError:
        print(f"Error: Input file not found at '{CSV_INPUT_FILE}'.")
        return

    print(f"Found {len(unique_pks)} unique keys to query...")

    with open(CSV_OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Searched_PK', 'Found_Item_Body'])

        for i, pk_value in enumerate(unique_pks):
            print(f"Querying records for PK {i + 1}/{len(unique_pks)}: {pk_value}")
            items = query_items_by_pk(pk_value)

            if items is None:
                writer.writerow([pk_value, 'ERROR_DURING_QUERY'])
                continue

            if items:
                for item in items:
                    transformed_item = transform_item(item)
                    json_body = json.dumps(transformed_item, cls=DecimalEncoder, indent=4, ensure_ascii=False)
                    writer.writerow([pk_value, json_body])
            else:
                writer.writerow([pk_value, 'NOT_FOUND'])

    print("Process completed successfully! ✨")


if __name__ == '__main__':
    main()