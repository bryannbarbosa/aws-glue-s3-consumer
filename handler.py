import json
import boto3

def trigger(event, context):
    for s3_event in event['Records']:
        glue_client = boto3.client('glue')
        glue_client.start_job_run(
            JobName='serverless-glue-import-job',
            Arguments={
                '--s3_trigger_event': json.dumps(s3_event)
            }
        )

        print(f"Glue job triggered for: {s3_event['s3']['object']['key']}")
