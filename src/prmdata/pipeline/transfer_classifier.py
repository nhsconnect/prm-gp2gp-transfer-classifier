from datetime import datetime, timedelta
from typing import Iterator

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import (
    TransferObservabilityProbe,
    TransferService,
    module_logger,
)
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_day
from prmdata.domain.spine.message import Message
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierS3UriResolver
from prmdata.utils.input_output.s3 import S3DataManager


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

        output_metadata = {
            "cutoff-days": str(config.conversation_cutoff.days),
            "build-tag": config.build_tag,
            "start-datetime": config.start_datetime.isoformat()
            if config.start_datetime
            else "None",
            "end-datetime": config.end_datetime.isoformat() if config.end_datetime else "None",
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

    def run(self):
        transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
        spine_messages = self._read_spine_messages()

        transfer_service = TransferService(
            message_stream=spine_messages,
            cutoff=self._cutoff,
            observability_probe=transfer_observability_probe,
        )

        conversations = transfer_service.group_into_conversations()
        gp2gp_conversations = transfer_service.parse_conversations_into_gp2gp_conversations(
            conversations
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
