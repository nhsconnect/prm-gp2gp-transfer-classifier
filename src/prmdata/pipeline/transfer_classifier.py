from datetime import datetime, timedelta
from logging import Logger, getLogger
from typing import Dict, Iterator, List

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import TransferService, TransferServiceObservabilityProbe
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_day
from prmdata.domain.spine.message import Message
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.pipeline.s3_uri_resolver import TransferClassifierS3UriResolver
from prmdata.utils.date_converter import convert_to_datetime_string, convert_to_datetimes_string
from prmdata.utils.input_output.s3 import JsonFileNotFoundException, S3DataManager

module_logger = getLogger(__name__)


class RunnerObservabilityProbe:
    def __init__(
        self,
        config: TransferClassifierConfig,
        reporting_window: ReportingWindow,
        logger: Logger = module_logger,
    ):
        self._logger = logger
        self._log_date_range_info = self._construct_json_log_date_range_info(
            config, reporting_window
        )

    @staticmethod
    def _construct_json_log_date_range_info(
        config: TransferClassifierConfig, reporting_window: ReportingWindow
    ) -> dict:
        reporting_window_dates = reporting_window.get_dates()
        reporting_window_overflow_dates = reporting_window.get_overflow_dates()
        return {
            "config_start_datetime": convert_to_datetime_string(config.start_datetime),
            "config_end_datetime": convert_to_datetime_string(config.end_datetime),
            "conversation_cutoff": str(config.conversation_cutoff),
            "reporting_window_dates": convert_to_datetimes_string(reporting_window_dates),
            "reporting_window_overflow_dates": convert_to_datetimes_string(
                reporting_window_overflow_dates
            ),
        }

    def log_attempting_to_classify(self):
        self._logger.info(
            "Attempting to classify conversations for a date range",
            extra={
                "event": "ATTEMPTING_CLASSIFY_CONVERSATIONS_FOR_A_DATE_RANGE",
                **self._log_date_range_info,
            },
        )

    def log_using_previous_month_ods_metadata(self, missing_json_uri: str):
        self._logger.warning(
            "Current month ODS metadata not found, falling back to previous month ODS metadata",
            extra={
                "event": "USING_PREVIOUS_MONTH_ODS_METADATA",
                "missing_json_uri": missing_json_uri,
            },
        )

    def log_successfully_classified(self, ods_metadata_input_paths: List[str]):
        self._logger.info(
            "Successfully classified conversations for a date range",
            extra={
                "event": "CLASSIFIED_CONVERSATIONS_FOR_A_DATE_RANGE",
                "ods_metadata_s3_paths_used": ods_metadata_input_paths,
                **self._log_date_range_info,
            },
        )

    def log_previous_month_ods_metadata_not_found(self, missing_json_uri: str):
        self._logger.error(
            f"Previous month ODS metadata not found: {missing_json_uri}, exiting...",
            extra={
                "event": "MISSING_PREVIOUS_MONTH_ODS_METADATA",
                "missing_json_uri": missing_json_uri,
            },
        )


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
        self._runner_observability_probe = RunnerObservabilityProbe(
            self._config, self._reporting_window
        )

        transfer_service_observability_probe = TransferServiceObservabilityProbe(
            logger=module_logger
        )
        self._transfer_service = TransferService(
            cutoff=self._config.conversation_cutoff,
            observability_probe=transfer_service_observability_probe,
        )

    def _read_previous_month_ods_metadata(
        self, missing_json_uri: str
    ) -> OrganisationMetadataMonthly:
        try:
            input_paths = self._uris.ods_metadata_using_previous_month(
                self._reporting_window.get_dates()
            )
            self._ods_metadata_input_paths = input_paths
            self._runner_observability_probe.log_using_previous_month_ods_metadata(missing_json_uri)
            return self._io.read_ods_metadata_files(input_paths)
        except JsonFileNotFoundException as e:
            self._runner_observability_probe.log_previous_month_ods_metadata_not_found(
                e.missing_json_uri
            )
            raise e

    def _read_spine_messages(self) -> Iterator[Message]:
        input_paths = self._uris.spine_messages(self._reporting_window)
        return self._io.read_spine_messages(input_paths)

    def _read_most_recent_ods_metadata(self) -> OrganisationMetadataMonthly:
        try:
            input_paths = self._uris.ods_metadata(self._reporting_window.get_dates())
            self._ods_metadata_input_paths = input_paths
            return self._io.read_ods_metadata_files(input_paths)
        except JsonFileNotFoundException as e:
            return self._read_previous_month_ods_metadata(e.missing_json_uri)

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

    def run(self):
        self._runner_observability_probe.log_attempting_to_classify()

        spine_messages = self._read_spine_messages()
        ods_metadata_monthly = self._read_most_recent_ods_metadata()

        conversations = self._transfer_service.group_into_conversations(
            message_stream=spine_messages
        )
        gp2gp_conversations = self._transfer_service.parse_conversations_into_gp2gp_conversations(
            conversations
        )

        for daily_start_datetime in self._reporting_window.get_dates():
            conversations_started_in_reporting_window = filter_conversations_by_day(
                gp2gp_conversations, daily_start_datetime
            )
            organisation_lookup = ods_metadata_monthly.get_lookup(
                (daily_start_datetime.year, daily_start_datetime.month)
            )
            transfers = self._transfer_service.convert_to_transfers(
                conversations_started_in_reporting_window, organisation_lookup=organisation_lookup
            )

            metadata = {
                "cutoff-days": str(self._config.conversation_cutoff.days),
                "build-tag": self._config.build_tag,
                "start-datetime": convert_to_datetime_string(daily_start_datetime),
                "end-datetime": convert_to_datetime_string(
                    daily_start_datetime + timedelta(days=1)
                ),
                "ods-metadata-month": f"{organisation_lookup.year}-{organisation_lookup.month}",
            }

            self._write_transfers(
                transfers=transfers,
                daily_start_datetime=daily_start_datetime,
                cutoff=self._config.conversation_cutoff,
                metadata=metadata,
            )

        self._runner_observability_probe.log_successfully_classified(
            ods_metadata_input_paths=self._ods_metadata_input_paths
        )
