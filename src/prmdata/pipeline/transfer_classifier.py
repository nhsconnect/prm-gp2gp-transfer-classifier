from datetime import datetime, timedelta
from logging import getLogger
from typing import Iterator

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import TransferService
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_day
from prmdata.domain.spine.message import Message
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierS3UriResolver
from prmdata.utils.date_converter import convert_to_datetime_string, convert_to_datetimes_string
from prmdata.utils.input_output.s3 import S3DataManager

logger = getLogger(__name__)


class TransferClassifier:
    def __init__(self, config: TransferClassifierConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = ReportingWindow(
            config.start_datetime, config.end_datetime, config.conversation_cutoff
        )

        self._cutoff = config.conversation_cutoff

        self._uris = TransferClassifierS3UriResolver(
            gp2gp_spine_bucket=config.input_spine_data_bucket,
            transfers_bucket=config.output_transfer_data_bucket,
        )

        self._start_datetime_config_string = convert_to_datetime_string(config.start_datetime)
        self._end_datetime_config_string = convert_to_datetime_string(config.end_datetime)
        output_metadata = {
            "cutoff-days": str(config.conversation_cutoff.days),
            "build-tag": config.build_tag,
            "start-datetime": self._start_datetime_config_string,
            "end-datetime": self._end_datetime_config_string,
        }

        self._io = TransferClassifierIO(s3_manager, output_metadata)

    def _read_spine_messages(self) -> Iterator[Message]:
        input_paths = self._uris.spine_messages(self._reporting_window)
        return self._io.read_spine_messages(input_paths)

    def _write_transfers(
        self, transfers: Iterator[Transfer], daily_start_datetime: datetime, cutoff: timedelta
    ):
        output_path = self._uris.gp2gp_transfers(
            daily_start_datetime=daily_start_datetime, cutoff=cutoff
        )
        self._io.write_transfers(transfers, output_path)

    def _construct_json_log_date_range_info(self) -> dict:
        reporting_window_dates = self._reporting_window.get_dates()
        reporting_window_overflow_dates = self._reporting_window.get_overflow_dates()
        return {
            "config_start_datetime": self._start_datetime_config_string,
            "config_end_datetime": self._end_datetime_config_string,
            "reporting_window_dates": convert_to_datetimes_string(reporting_window_dates),
            "reporting_window_overflow_dates": convert_to_datetimes_string(
                reporting_window_overflow_dates
            ),
        }

    def run(self):
        spine_messages = self._read_spine_messages()

        transfer_service = TransferService(
            message_stream=spine_messages,
            cutoff=self._cutoff,
        )

        conversations = transfer_service.group_into_conversations()
        gp2gp_conversations = transfer_service.parse_conversations_into_gp2gp_conversations(
            conversations
        )

        logger.info(
            "Attempting to classify conversations for a date range",
            extra={
                "event": "ATTEMPTING_CLASSIFY_CONVERSATIONS_FOR_A_DATE_RANGE",
                **self._construct_json_log_date_range_info(),
            },
        )

        for daily_start_datetime in self._reporting_window.get_dates():
            conversations_started_in_reporting_window = filter_conversations_by_day(
                gp2gp_conversations, daily_start_datetime
            )
            transfers = transfer_service.convert_to_transfers(
                conversations_started_in_reporting_window
            )
            self._write_transfers(
                transfers=transfers, daily_start_datetime=daily_start_datetime, cutoff=self._cutoff
            )
