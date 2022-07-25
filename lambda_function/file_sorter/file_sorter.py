"""
This Module contains the FileSorter class that will sort the files into the appropriate
HERMES instrument folder.

TODO: Skeleton Code for initial repo, class still needs to be implemented including
logging to DynamoDB + S3 log file and docstrings expanded
"""

from hermes_core import log
from hermes_core.util import util
import boto3
import botocore


INSTRUMENT_BUCKET_NAMES = {
    "eea": "hermes-eea", 
    "nemesis": "hermes-nemsis",
    "merit": "hermes-merit",
    "spani": "hermes-spani"
}

class FileSorter:
    """
    Main FileSorter class which initializes an object with the data file and the
    bucket event which triggered the lambda function to be called.
    """

    def __init__(self, s3_bucket, s3_object):
        """
        FileSorter Constructorlogger
        """

        # Initialize Class Variables
        try:
            self.incoming_bucket_name = s3_bucket["name"]
            log.info(f"Incoming Bucket Name Parsed Successfully: {self.incoming_bucket_name}")

        except KeyError:
            error_message = "KeyError when extracting S3 Bucket Name/ARN from dict"
            log.error(error_message)
            raise KeyError(error_message)

        try:
            self.file_key = s3_object["key"]
            self.file_etag = s3_object["eTag"]

            log.info(f"Incoming Object Name Parsed Successfully: {self.file_key}")
            log.info(f"Incoming Object eTag Parsed Successfully: {self.file_etag}")

        except KeyError:
            error_message = "KeyError when extracting S3 Object Name/eTag from dict"
            log.error(error_message)
            raise KeyError(error_message)

        # Call sort function
        self._sort_file()

    def _sort_file(self):
        """
        Function that chooses calls correct sorting function
        based off file key name.
        """

        # Dict of parsed science file
        self.destination_bucket = self._get_destination_bucket()

        # Copy file to destination bucket
        self._copy_from_incoming_to_destination()

    def _get_destination_bucket(self):
        """
        Returns bucket in which the file will be sorted to
        """
        try:
            science_file = util.parse_science_filename(self.file_key)
            destination_bucket = INSTRUMENT_BUCKET_NAMES[science_file["instrument"]]
            log.info(f"Destination Bucket Parsed Successfully: {destination_bucket}")
            
            return destination_bucket

        except ValueError as e:
            log.error(e)

            raise ValueError(e)

    def _copy_from_incoming_to_destination(self):

        """
        Function to copy file from S3 incoming bucket using bucket key
        to destination bucket
        """
        log.info(f"Initiating Copying of File From {self.incoming_bucket_name} to {self.destination_bucket}")

        try:
            # Initialize S3 Client and Copy Source Dict
            s3 = boto3.resource("s3")
            copy_source = {"Bucket": self.incoming_bucket_name, "Key": self.file_key}

            # Copy S3 file from incoming bucket to destination bucket
            s3.meta.client.copy(copy_source, self.destination_bucket, self.file_key)
            log.info(f"File {self.file_key} Successfully Moved to {self.destination_bucket}")

        except botocore.exceptions.ClientError as e:
            log.error(e)

            raise e

