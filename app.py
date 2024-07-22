from flask import Flask
import boto3
import json
import os
import logging

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# Environment variables with defaults
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', 'mock_access_key')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', 'mock_secret_key')
REGION_NAME = os.getenv('REGION_NAME', 'eu-west-1')
ENDPOINTS_URL = os.getenv('ENDPOINTS_URL', 'http://localstack:4566')
QUEUE_URL = os.getenv('QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'my_bucket')

# Initialize clients
sqs = boto3.client('sqs', region_name=REGION_NAME, endpoint_url=ENDPOINTS_URL, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
s3 = boto3.client('s3', region_name=REGION_NAME, endpoint_url=ENDPOINTS_URL, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def process_sqs_messages():
    app.logger.debug('Starting to process SQS messages')
    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        app.logger.debug(f'Received response: {response}')
        
        if 'Messages' in response:
            for message in response['Messages']:
                app.logger.debug(f'Message body: {message["Body"]}')
                try:
                    data = json.loads(message['Body'])
                    
                    # Check if the data is directly in the message body or wrapped in a 'data' key
                    if 'data' in data:
                        data = data['data']
                    app.logger.debug(f'Parsed data: {data}')
                    
                    # Upload to S3
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=f"{data['email_timestream']}.json",
                        Body=json.dumps(data)
                    )
                    app.logger.debug(f'Uploaded data to S3 with key: {data["email_timestream"]}.json')

                    # Delete message from SQS
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    app.logger.debug('Deleted message from SQS')
                except json.JSONDecodeError as e:
                    app.logger.error(f'JSON decode error: {e}')
                except KeyError as e:
                    app.logger.error(f'Missing key in data: {e}')
                except Exception as e:
                    app.logger.error(f'Error processing message: {e}')
        else:
            app.logger.debug('No messages found')

                    # Delete message from SQS
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    app.logger.debug('Deleted message from SQS')
                except Exception as e:
                    app.logger.error(f'Error processing message: {e}')
        else:
            app.logger.debug('No messages found')
            
@app.route('/start', methods=['GET'])
def start_processing():
    process_sqs_messages()
    return "Started processing SQS messages", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
