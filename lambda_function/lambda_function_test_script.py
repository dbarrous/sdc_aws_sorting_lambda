"""
Script to be used for testing the lambda handler locally
"""
# from lambda_function import handler
from hermes_core.util import util  # noqa: E402

if __name__ == "__main__":
    # Set Event to Lambda Handler With
    event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2022-07-25T09:35:08.284Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "AWS:AIDAVD4XLJ3QS3NKL4PSC"},
                "requestParameters": {"sourceIPAddress": "109.175.193.59"},
                "responseElements": {
                    "x-amz-request-id": "4H9A5X20QMSB7B5B",
                    "x-amz-id-2": "EHz4G7hn4dAREyeI5yQMYzkDYyfuowiwjMbG/KVsxeRGRmf3bS4DoQ2EY617fASV9FzhCviD6nPTcYJQeeyUvk8JY/WV7WXp",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "arn:aws:cloudformation:us-east-1:351967858401:stack/SDCAWSSortingLambdaStack/57843660-0bfc-11ed-86da-1231e463b7cd--7645585606264049987",
                    "bucket": {
                        "name": "swsoc-incoming",
                        "ownerIdentity": {"principalId": "A3V7OORH2511GS"},
                        "arn": "arn:aws:s3:::swsoc-incoming",
                    },
                    "object": {
                        "key": "hermes_spn_2s_l3test_burst_20240406_120621_v2.4",
                        "size": 68221,
                        "eTag": "32d82e8a2e72af004c557c4e369e89ff",
                        "sequencer": "0062DE63CC330B223E",
                    },
                },
            }
        ]
    }
    context = {}
    print(handler(event, context))
