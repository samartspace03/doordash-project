import json
import pandas as pd
import boto3
import io
import os

# Initialize boto3 clients outside handler for efficiency
s3 = boto3.client('s3')
sns = boto3.client('sns')

# You can set this as an environment variable in Lambda
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'doordash-target-zn-gds-assign3')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:ap-south-1:058264464054:s3-info')

def lambda_handler(event, context):
    try:
        # 1. Read bucket name and object key from S3 event
        input_bucket = event['Records'][0]['s3']['bucket']['name']
        input_key = event['Records'][0]['s3']['object']['key']

        # 2. Read the JSON file from S3
        response = s3.get_object(Bucket=input_bucket, Key=input_key)
        content = response['Body'].read().decode('utf-8')

        # Each line is a separate JSON object
        json_lines = content.strip().split('\n')
        data = [json.loads(line) for line in json_lines]

        # 3. Filter 'delivered' items and create DataFrame
        delivered_items = [item for item in data if item.get('status') == 'delivered']
        if not delivered_items:
            return {"message": "No delivered items found."}

        df = pd.DataFrame(delivered_items)

        # 4. Save DataFrame to CSV in-memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_file_name = f"delivered_items_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # 5. Upload CSV to target S3 bucket under processed_data/
        s3.put_object(
            Bucket=TARGET_BUCKET,
            Key=f"processed_data/{csv_file_name}",
            Body=csv_buffer.getvalue()
        )

        # 6. Send SNS notification
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=f"CSV file {csv_file_name} with delivered items uploaded to {TARGET_BUCKET}/processed_data/",
            Subject="Delivery Data Processed"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(f"Processed {len(delivered_items)} delivered items.")
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing S3 file: {str(e)}")
        }
