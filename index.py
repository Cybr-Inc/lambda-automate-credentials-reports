import json
import boto3
import time
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
iam = boto3.client('iam')
s3 = boto3.client('s3')

def wait_for_credential_report():
    """Wait for the credential report to be generated."""
    max_retries = 20  # Max retries
    retry_delay = 3   # Seconds between retries
    
    for attempt in range(max_retries):
        try:
            response = iam.get_credential_report()
            if response['ReportFormat'] == 'text/csv':
                logger.info("Successfully retrieved credential report")
                return response['Content']
        except iam.exceptions.CredentialReportNotPresentException:
            logger.info(f"Report not ready, attempt {attempt + 1}/{max_retries}")
            time.sleep(retry_delay)
        except iam.exceptions.CredentialReportNotReadyException:
            logger.info(f"Report still generating, attempt {attempt + 1}/{max_retries}")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error while getting credential report: {str(e)}")
            raise
            
    raise Exception(f"Timeout waiting for credential report after {max_retries} attempts")

def lambda_handler(event, context):
    try:
        # Extract bucket name from event
        bucket_name = event.get('bucket_name')
        if not bucket_name:
            raise ValueError("No bucket name provided in the event")

        # Generate credential report
        logger.info("Requesting credential report generation...")
        try:
            iam.generate_credential_report()
        except iam.exceptions.LimitExceededException:
            logger.info("Report generation already in progress, proceeding to wait...")

        # Wait for and get the credential report
        logger.info("Waiting for credential report to be ready...")
        report_content = wait_for_credential_report()

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'credential_report_{timestamp}.csv'

        # Upload to S3
        logger.info(f"Uploading report to S3 bucket: {bucket_name}")
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=report_content,
            ContentType='text/csv',
            ServerSideEncryption='AES256'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Credential report generated and uploaded successfully',
                'bucket': bucket_name,
                'filename': filename
            })
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
