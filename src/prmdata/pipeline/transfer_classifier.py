from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from logging import Logger, getLogger
from typing import Dict, Iterator, List

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import TransferService, TransferServiceObservabilityProbe
from prmdata.domain.mi.mi_message import MiMessage
from prmdata.domain.mi.mi_transfer import MiTransfer
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly
from prmdata.domain.reporting_window import ReportingWindow
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

    def log_successfully_read_mi_events(self, mi_events: List[List[dict]]):
        self._logger.info(
            "Successfully read mi events from s3",
            extra={
                "event": "SUCCESSFULLY_READ_MI_EVENTS",
                "events": str(mi_events),
                **self._log_date_range_info,
            },
        )

    def log_successfully_constructed_mi_messages(self, mi_messages: List[MiMessage]):
        self._logger.info(
            "Successfully constructed mi messages from mi events",
            extra={
                "event": "SUCCESSFULLY_CONSTRUCTED_MI_MESSAGES_FROM_MI_EVENTS",
                # "mi_messages": str(mi_messages),
                **self._log_date_range_info,
            },
        )

    def log_successfully_grouped_mi_messages(self, grouped_mi_messages: List[MiMessage]):
        self._logger.info(
            "Successfully grouped my messages by conversation ID",
            extra={
                "event": "SUCCESSFULLY_GROUPED_MI_MESSAGES_BY_CONVERSATION_ID",
                # "grouped_mi_messages": json.dumps(
                #     grouped_mi_messages, default=lambda o: o.__dict__, sort_keys=True
                # ),
                **self._log_date_range_info,
            },
        )

    def log_successfully_created_transfers_from_mi_events(self, mi_transfers: List[MiTransfer]):
        # self._logger.info(
        #     "Successfully created transfers from mi events",
        #     extra={
        #         "event": "SUCCESSFULLY_CREATED_TRANSFERS_FROM_MI_EVENTS",
        #         # "transfers": json.dumps(mi_transfers, default=lambda o: o.__dict__, sort_keys=True),
        #         **self._log_date_range_info,
        #     },
        # )

        self._logger.info(
            "Successfully created transfers from mi events",
            extra={
                "event": "SUCCESSFULLY_CREATED_TRANSFERS_FROM_MI_EVENTS",
                "transfers": str(mi_transfers),
                **self._log_date_range_info,
            },
        )


class TransferClassifier(ABC):
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
            mi_bucket=config.input_mi_data_bucket,
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

    @abstractmethod
    def run(self):
        pass
