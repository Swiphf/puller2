import boto3
import json
import os
import logging
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', 'mock_access_key')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', 'mock_secret_key')
REGION_NAME = os.getenv('REGION_NAME', 'eu-west-1')
ENDPOINTS_URL = os.getenv('ENDPOINTS_URL', 'http://localstack:4566')
QUEUE_URL = os.getenv('QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'my_bucket')

sqs = boto3.client('sqs', region_name=REGION_NAME, endpoint_url=ENDPOINTS_URL, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
s3 = boto3.client('s3', region_name=REGION_NAME, endpoint_url=ENDPOINTS_URL, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def check_queue_exists():
    try:
        sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=['All'])
        return True
    except sqs.exceptions.QueueDoesNotExist:
        logger.error('Queue does not exist.')
        return False

def process_sqs_messages():
    logger.debug('Starting to process SQS messages')
    while True:
        if not check_queue_exists():
            logger.debug('Retrying in 10 seconds...')
            time.sleep(10)
            continue
            
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        logger.debug(f'Received response: {response}')
        
        if 'Messages' in response:
            for message in response['Messages']:
                logger.debug(f'Message body: {message["Body"]}')
                try:
                    data = json.loads(message['Body'])
                    logger.debug(f'Parsed data: {data}')
                    
                    # Upload to S3
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=f"{data['email_timestream']}.json",
                        Body=json.dumps(data)
                    )
                    logger.debug(f'Uploaded data to S3 with key: {data["email_timestream"]}.json')

                    # Delete message from SQS
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    logger.debug('Deleted message from SQS')
                except json.JSONDecodeError as e:
                    logger.error(f'JSON decode error: {e}')
                except KeyError as e:
                    logger.error(f'Missing key in data: {e}')
                except Exception as e:
                    logger.error(f'Error processing message: {e}')
        else:
            logger.debug('No messages found')
        time.sleep(10)

if __name__ == '__main__':
    process_sqs_messages()
