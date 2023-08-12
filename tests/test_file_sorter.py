import os
import boto3
import pytest
from moto import mock_s3, mock_timestreamwrite
from sdc_aws_utils.aws import create_s3_file_key
from pathlib import Path
from slack_sdk.errors import SlackApiError

# Constants
os.environ["SDC_AWS_CONFIG_FILE_PATH"] = "lambda_function/config.yaml"
from lambda_function.file_sorter import file_sorter
from sdc_aws_utils.config import parser

INCOMING_BUCKET = "swsoc-incoming"
TEST_BUCKET = "hermes-spani"
TEST_BAD_FILE = "/tests/test_files/test-file-key.txt"
TEST_MISSING_FILE = "/tests/test_files/missing-file-key.txt"
TEST_L0_FILE = "/tests/test_files/hermes_SPANI_l0_2023040-000018_v01.bin"
TEST_REGION = "us-east-1"
ENVIRONMENT = "PRODUCTION"


# Fixtures
@pytest.fixture(scope="function")
def aws_credentials():
    """AWS Credentials fixture"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """S3 client fixture"""
    with mock_s3():
        conn = boto3.client("s3", region_name=TEST_REGION)
        conn.create_bucket(Bucket=TEST_BUCKET)
        yield conn


@pytest.fixture(scope="function")
def timestream_client(aws_credentials):
    """Timestream client fixture"""
    with mock_timestreamwrite():
        conn = boto3.client("timestream-write", region_name=TEST_REGION)
        yield conn


# Utility Functions
def create_s3_event(bucket_name, object_key):
    """Create S3 Event"""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2023-08-12T12:34:56.789Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE",
                    "x-amz-id-2": "EXAMPLE",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": bucket_name,
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": "arn:aws:s3:::{}".format(bucket_name),
                    },
                    "object": {
                        "key": object_key,
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }


def setup_environment(s3_client, timestream_client):
    """Utility function to set up the environment for testing."""

    # Create buckets in S3
    s3_client.create_bucket(Bucket=TEST_BUCKET)
    s3_client.create_bucket(Bucket=INCOMING_BUCKET)
    s3_client.put_object(Bucket=INCOMING_BUCKET, Key=TEST_L0_FILE, Body=b"test file")
    s3_client.put_object(Bucket=INCOMING_BUCKET, Key=TEST_BAD_FILE, Body=b"test file")

    # Set up the database and table in Timestream
    try:
        timestream_client.create_database(DatabaseName="sdc_aws_logs")
    except timestream_client.exceptions.ConflictException:
        pass
    try:
        timestream_client.create_table(
            DatabaseName="sdc_aws_logs", TableName="sdc_aws_s3_bucket_log_table"
        )
    except timestream_client.exceptions.ConflictException:
        pass

    os.environ["LAMBDA_ENVIRONMENT"] = ENVIRONMENT


# Tests handle_event Function
@mock_s3
def test_handle_event_s3_event(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    # Create S3 event
    s3_event = create_s3_event(INCOMING_BUCKET, TEST_L0_FILE)

    # Test normal successful run
    response = file_sorter.handle_event(event=s3_event, context=None)

    assert response["statusCode"] == 200

    # Test Error handling
    s3_event = create_s3_event(INCOMING_BUCKET, TEST_BAD_FILE)
    response = file_sorter.handle_event(event=s3_event, context=None)

    assert response["statusCode"] == 500


@mock_s3
def test_handle_event_trigger(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    # Create trigger event
    trigger_event = {}

    # Test normal successful run of trigger event
    response = file_sorter.handle_event(event=trigger_event, context=None)

    assert response["statusCode"] == 200

    # Test already existing file
    s3_client.put_object(Bucket=TEST_BUCKET, Key=TEST_L0_FILE, Body=b"test file")
    response = file_sorter.handle_event(event=trigger_event, context=None)

    assert response["statusCode"] == 200


# Tests FileSorter class
@mock_s3
def test_file_sorter(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    file_sorter.FileSorter(
        s3_bucket=INCOMING_BUCKET,
        file_key=TEST_L0_FILE,
        environment=ENVIRONMENT,
        dry_run=True,
        s3_client=s3_client,
        timestream_client=timestream_client,
    )

    # Check that the file was not copied during a dry run
    assert not s3_client.list_objects(Bucket=TEST_BUCKET).get("Contents")

    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_L0_FILE,
            ENVIRONMENT,
            dry_run=False,
            s3_client=s3_client,
            timestream_client=timestream_client,
        )
    except FileNotFoundError as e:
        assert e is not None

    path_file = Path(TEST_L0_FILE).name

    # Check that the file was copied to the correct HERMES folder
    assert s3_client.list_objects(
        Bucket=TEST_BUCKET,
    ).get(
        "Contents"
    )[0].get(
        "Key"
    ) == create_s3_file_key(parser, path_file)


@mock_s3
def test_file_sorter_with_slack(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    file_sorter.FileSorter(
        s3_bucket=INCOMING_BUCKET,
        file_key=TEST_L0_FILE,
        environment=ENVIRONMENT,
        dry_run=True,
        s3_client=s3_client,
        timestream_client=timestream_client,
        slack_token="test-token",
        slack_channel="test-channel",
    )

    # Check that the file was not copied during a dry run
    assert not s3_client.list_objects(Bucket=TEST_BUCKET).get("Contents")

    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_L0_FILE,
            "",
            dry_run=False,
            s3_client=s3_client,
            timestream_client=timestream_client,
            slack_token="test-token",
            slack_channel="test-channel",
        )
    except SlackApiError as e:
        assert e is not None
    path_file = Path(TEST_L0_FILE).name

    # Check that the file was copied to the correct HERMES folder
    assert s3_client.list_objects(
        Bucket=TEST_BUCKET,
    ).get(
        "Contents"
    )[0].get(
        "Key"
    ) == create_s3_file_key(parser, path_file)


@mock_s3
def test_file_sorter_missing_file(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_MISSING_FILE,
            ENVIRONMENT,
            dry_run=True,
        )

    except ValueError as e:
        assert e is not None

    assert not s3_client.list_objects(Bucket=TEST_BUCKET).get("Contents")


@mock_s3
def test_file_sorter_bad_file(s3_client, timestream_client):
    # Set up environment
    setup_environment(s3_client, timestream_client)

    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_BAD_FILE,
            ENVIRONMENT,
            dry_run=True,
        )

    except ValueError as e:
        assert e is not None

    assert not s3_client.list_objects(Bucket=TEST_BUCKET).get("Contents")


@mock_s3
def test_file_sorter_missing_s3_bucket(s3_client):
    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_L0_FILE,
            ENVIRONMENT,
            dry_run=False,
            s3_client=s3_client,
        )
    except Exception as e:
        assert e is not None


@mock_s3
def test_file_sorter_missing_timestream(s3_client):
    s3_client.create_bucket(Bucket=TEST_BUCKET)
    s3_client.create_bucket(Bucket=INCOMING_BUCKET)
    s3_client.put_object(Bucket=INCOMING_BUCKET, Key=TEST_L0_FILE, Body=b"test file")
    try:
        file_sorter.FileSorter(
            INCOMING_BUCKET,
            TEST_L0_FILE,
            ENVIRONMENT,
            dry_run=False,
            s3_client=s3_client,
        )
    except Exception as e:
        assert e is not None

    assert s3_client.list_objects(Bucket=TEST_BUCKET).get("Contents")
