# from datetime import timedelta
# from unittest.mock import Mock
#
# import boto3
# from moto import mock_s3
#
# from prmdata.pipeline.config import TransferClassifierConfig
# from prmdata.pipeline.io import TransferClassifierIO
# from prmdata.pipeline.spine_runner import SpineRunner
# from prmdata.utils.input_output.s3 import S3DataManager
# from tests.unit.utils.io.s3 import MOTO_MOCK_REGION

# @mock_s3
# def test_transfer_classifier_spine_runner_abstract_class():
#     conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
#     conn.create_bucket(Bucket="test_bucket")
#
#     Mock(TransferClassifierIO)
#     Mock(S3DataManager)
#
#     config = TransferClassifierConfig(
#         input_spine_data_bucket="test_bucket",
#         output_transfer_data_bucket="test_bucket",
#         input_ods_metadata_bucket="test_bucket",
#         start_datetime=None,
#         end_datetime=None,
#         s3_endpoint_url=None,
#         conversation_cutoff=timedelta(days=0),
#         build_tag="12345",
#     )
#     SpineRunner(config).run()
