import logging
from datetime import datetime, timedelta
from typing import Dict, Iterator

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import (
    TransferObservabilityProbe,
    TransferService,
    module_logger,
)
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_day
from prmdata.domain.spine.message import Message
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierS3UriResolver
from prmdata.utils.date_converter import convert_to_datetime_string, convert_to_datetimes_string
from prmdata.utils.input_output.s3 import S3DataManager

logger = logging.getLogger(__name__)


class TransferClassifier:
    def __init__(self, config: TransferClassifierConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = ReportingWindow(
            config.start_datetime, config.end_datetime, config.conversation_cutoff
        )

        self._config = config

        self._uris = TransferClassifierS3UriResolver(
            gp2gp_spine_bucket=config.input_spine_data_bucket,
            transfers_bucket=config.output_transfer_data_bucket,
            ods_metadata_bucket=config.input_ods_metadata_bucket,
        )

        self._io = TransferClassifierIO(s3_manager)

    def _read_spine_messages(self) -> Iterator[Message]:
        input_paths = self._uris.spine_messages(self._reporting_window)
        return self._io.read_spine_messages(input_paths)

    def _read_ods_metadata(self) -> OrganisationMetadataMonthly:
        input_paths = self._uris.ods_metadata(self._reporting_window)
        return self._io.read_ods_metadata_files(input_paths)

    def _write_transfers(
        self,
        transfers: Iterator[Transfer],
        daily_start_datetime: datetime,
        cutoff: timedelta,
        metadata: Dict[str, str],
    ):
        output_path = self._uris.gp2gp_transfers(
            daily_start_datetime=daily_start_datetime, cutoff=cutoff
        )
        self._io.write_transfers(transfers, output_path, metadata)

    def _write_transfers_deprecated(
        self,
        transfers: Iterator[Transfer],
        daily_start_datetime: datetime,
        cutoff: timedelta,
        metadata: Dict[str, str],
    ):
        output_path = self._uris.gp2gp_transfers_deprecated(
            daily_start_datetime=daily_start_datetime, cutoff=cutoff
        )
        self._io.write_transfers_deprecated(transfers, output_path, metadata)

    def _construct_json_log_date_range_info(self) -> dict:
        reporting_window_dates = self._reporting_window.get_dates()
        reporting_window_overflow_dates = self._reporting_window.get_overflow_dates()
        return {
            "config_start_datetime": convert_to_datetime_string(self._config.start_datetime),
            "config_end_datetime": convert_to_datetime_string(self._config.end_datetime),
            "conversation_cutoff": str(self._config.conversation_cutoff),
            "reporting_window_dates": convert_to_datetimes_string(reporting_window_dates),
            "reporting_window_overflow_dates": convert_to_datetimes_string(
                reporting_window_overflow_dates
            ),
        }

    def run(self):
        transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)

        log_date_range_info = self._construct_json_log_date_range_info()
        logger.info(
            "Attempting to classify conversations for a date range",
            extra={
                "event": "ATTEMPTING_CLASSIFY_CONVERSATIONS_FOR_A_DATE_RANGE",
                **log_date_range_info,
            },
        )

        spine_messages = self._read_spine_messages()
        ods_metadata_monthly = self._read_ods_metadata()

        transfer_service = TransferService(
            message_stream=spine_messages,
            cutoff=self._config.conversation_cutoff,
            observability_probe=transfer_observability_probe,
        )

        conversations = transfer_service.group_into_conversations()
        gp2gp_conversations = transfer_service.parse_conversations_into_gp2gp_conversations(
            conversations
        )

        for daily_start_datetime in self._reporting_window.get_dates():
            metadata = {
                "cutoff-days": str(self._config.conversation_cutoff.days),
                "build-tag": self._config.build_tag,
                "start-datetime": convert_to_datetime_string(daily_start_datetime),
                "end-datetime": convert_to_datetime_string(
                    daily_start_datetime + timedelta(days=1)
                ),
                "ods-metadata-month": f"{daily_start_datetime.year}-{daily_start_datetime.month}",
            }

            conversations_started_in_reporting_window = filter_conversations_by_day(
                gp2gp_conversations, daily_start_datetime
            )
            organisation_lookup = ods_metadata_monthly.get_lookup(
                (daily_start_datetime.year, daily_start_datetime.month)
            )
            transfers = transfer_service.convert_to_transfers(
                conversations_started_in_reporting_window, organisation_lookup=organisation_lookup
            )
            if self._config.add_ods_codes == 1:
                self._write_transfers(
                    transfers=transfers,
                    daily_start_datetime=daily_start_datetime,
                    cutoff=self._config.conversation_cutoff,
                    metadata=metadata,
                )
            else:
                self._write_transfers_deprecated(
                    transfers=transfers,
                    daily_start_datetime=daily_start_datetime,
                    cutoff=self._config.conversation_cutoff,
                    metadata=metadata,
                )

        logger.info(
            "Successfully classified conversations for a date range",
            extra={
                "event": "CLASSIFIED_CONVERSATIONS_FOR_A_DATE_RANGE",
                **log_date_range_info,
            },
        )
