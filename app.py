from flask import Flask
import boto3
import time
import json

app = Flask(__name__)


AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', 'mock_access_key')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', 'mock_secret_key')

# Initialize SQS and S3 clients
sqs = boto3.client('sqs', region_name='eu-west-1', endpoint_url='http://localhost:4566', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
s3 = boto3.client('s3', region_name='eu-west-1', endpoint_url='http://localhost:4566', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Your SQS queue URL and S3 bucket name
QUEUE_URL = 'http://localhost:4566/000000000000/my-queue'
BUCKET_NAME = 'my-bucket'

def process_sqs_messages():
    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if 'Messages' in response:
            for message in response['Messages']:
                data = json.loads(message['Body'])
                # Upload the message to S3
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=f"{data['email_timestream']}.json",
                    Body=json.dumps(data)
                )
                # Delete the message from SQS
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

@app.route('/start', methods=['GET'])
def start_processing():
    process_sqs_messages()
    return "Started processing SQS messages", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
