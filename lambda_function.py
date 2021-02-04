from __future__ import print_function

import boto3
from decimal import Decimal
import json
from urllib.parse import unquote_plus

print('Loading function')

# Client services
rekognition = boto3.client('rekognition')
client = boto3.client('sns')
s3_client = boto3.client('s3')

# --------------- Helper Functions to call Rekognition APIs ------------------
labelsSet = {"Dog", "Cat", "Apple", "Banana", "Cherry", "Pumpkin", "Onion", "Potato"}


# Returns the label with highest confidence and present in a predefined set of labels
def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    for Labels in response['Labels']:
        if (Labels["Name"] in labelsSet) and Labels["Confidence"] > 90:
            return True, [Labels["Name"], Labels["Confidence"]]
    return False, []


# --------------- Main handler ------------------


def lambda_handler(event, context):
    '''Demonstrates S3 trigger that uses
    Rekognition APIs to detect faces, labels and index faces in S3 Object.
    '''
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    record = event['Records'][0]
    x = record.get('body')
    if isinstance(x, str):
        x = json.loads(x)
    for key in x.keys():
        y = x.get(key)
        for r in y:
            s3Obj = r.get('s3')
            bucket = r.get('s3').get('bucket').get('name')
            key = s3Obj.get('object').get('key')
            if '+' in key:
                key = unquote_plus(key)
            try:

                # Calls rekognition DetectLabels API to detect labels in S3 object
                response, results = detect_labels(bucket, key)

                if response:
                    filepath = '/tmp/' + key

                    # Download files from s3 to lambda tmp folder . Lambda provides 512mb of tmp storage
                    s3_client.download_file(bucket, key, filepath)

                    # The downloaded file is then uploaded to a new s3
                    s3_client.upload_file(filepath, "analyzed-image-bucket", filepath.split("/")[-1])

                    success_target_arn = "arn:aws:sns:us-east-1:365848237714:success-topic-sns"
                    body = "Image detected: " + results[0] + " with a confidence of: " + str(results[1])
                    message = client.publish(TargetArn=success_target_arn, Message=body, Subject='SUCCESS')

                else:

                    failure_target_arn = "arn:aws:sns:us-east-1:365848237714:failure-topic-sns"
                    message = client.publish(TargetArn=failure_target_arn, Message='Detected nothing',
                                             Subject='FAILURE')

                return response
            except Exception as e:
                print(e)
                print("Error processing object {} from bucket {}. ".format(key, bucket) +
                      "Make sure your object and bucket exist and your bucket is in the same region as this function.")
                raise e
