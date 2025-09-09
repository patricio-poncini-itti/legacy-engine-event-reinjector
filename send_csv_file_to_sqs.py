import csv
import json
import boto3
import uuid
from botocore.exceptions import ClientError

INPUT_FILE = 'output_data.csv' # El archivo output que generó el script get_records.py
SQS_QUEUE_URL = 'url-of-the-sqs'
AWS_REGION = 'your-aws-region'

sqs_client = boto3.client('sqs', region_name=AWS_REGION)

def publicar_en_sqs(mensaje_dict):
    """
    Esta función publica un mensaje en una cola SQS FIFO.
    """
    try:
        grupo_id = mensaje_dict.get('idempotencyKey')

        if not grupo_id:
            print(f"❌ Error: 'idempotencyKey' no encontrada o es nula en el mensaje. Saltando...")
            return False

        print(json.dumps(mensaje_dict, indent=4, ensure_ascii=False))

        deduplicacion_id = str(uuid.uuid4())
        cuerpo_mensaje = json.dumps(mensaje_dict)

        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=cuerpo_mensaje,
            MessageGroupId=grupo_id,
            MessageDeduplicationId=deduplicacion_id
        )
        print(f"✅ Mensaje enviado a SQS para PK {grupo_id}. MessageId: {response['MessageId']}")
        return True

    except ClientError as e:
        print(f"❌ Error al enviar a SQS para PK {grupo_id}: {e.response['Error']['Message']}")
        return False
    except KeyError:
        print(f"❌ Error: 'idempotencyKey' no encontrada en el JSON del archivo CSV.")
        return False


def main():
    """
    Función principal que lee el CSV y publica su contenido directamente en SQS.
    """
    print(f"Iniciando proceso de envío a SQS desde el archivo '{INPUT_FILE}'...")

    try:
        with open(INPUT_FILE, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)

            for row in reader:
                pk_leida = row[0]
                json_string_leido = row[1]

                try:
                    mensaje_a_enviar = json.loads(json_string_leido)

                    publicar_en_sqs(mensaje_a_enviar)

                except json.JSONDecodeError:
                    print(f"⚠️  Saltando fila para PK {pk_leida} porque el body no es un JSON válido.")
                except Exception as e:
                    print(f"🚨 Error inesperado procesando la PK {pk_leida}: {e}")

    except FileNotFoundError:
        print(f"🚨 Error: No se encontró el archivo de entrada '{INPUT_FILE}'.")
        return

    print("\nProceso finalizado.")


if __name__ == '__main__':
    main()