import logging
from datetime import datetime
from os import environ
from typing import Iterator, List

import boto3

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.domain.monthly_reporting_window import MonthlyReportingWindow
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.message import Message
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import (
    TransferClassifierIO,
    TransferClassifierMonthlyS3UriResolver,
    TransferClassifierS3UriResolver,
)
from prmdata.pipeline.parse_transfers_from_messages import (
    parse_transfers_from_messages,
    parse_transfers_from_messages_monthly,
)
from prmdata.utils.io.json_formatter import JsonFormatter
from prmdata.utils.io.s3 import S3DataManager

logger = logging.getLogger("prmdata")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class TransferClassifierMonthlyPipeline:
    def __init__(self, config: TransferClassifierConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)

        self._cutoff = config.conversation_cutoff

        self._uris = TransferClassifierMonthlyS3UriResolver(
            gp2gp_spine_bucket=config.input_spine_data_bucket,
            transfers_bucket=config.output_transfer_data_bucket,
        )

        output_metadata = {
            "date-anchor": config.date_anchor.isoformat(),
            "cutoff-days": str(config.conversation_cutoff.days),
            "build-tag": config.build_tag,
        }

        self._io = TransferClassifierIO(s3_manager, output_metadata)

    def _read_spine_messages(self, metric_month, overflow_month):
        input_path = self._uris.spine_messages(metric_month, overflow_month)
        return self._io.read_spine_messages(input_path)

    def _write_transfers(self, transfers, metric_month):
        output_path = self._uris.gp2gp_transfers(metric_month)
        self._io.write_transfers(transfers, output_path)

    def run(self):
        metric_month = self._reporting_window.metric_month
        overflow_month = self._reporting_window.overflow_month
        spine_messages = self._read_spine_messages(metric_month, overflow_month)
        transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
        transfers = parse_transfers_from_messages_monthly(
            spine_messages=spine_messages,
            reporting_window=self._reporting_window,
            conversation_cutoff=self._cutoff,
            observability_probe=transfer_observability_probe,
        )
        self._write_transfers(transfers, metric_month)


class TransferClassifierPipeline:
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
            "start-datetime": config.start_datetime.isoformat(),
            "end-datetime": config.end_datetime.isoformat(),
        }

        self._io = TransferClassifierIO(s3_manager, output_metadata)

    def _read_spine_messages(self) -> List[Message]:
        input_paths = self._uris.spine_messages(self._reporting_window)
        return self._io.read_spine_messages(input_paths)

    def _write_transfers(self, transfers: Iterator[Transfer], daily_start_datetime: datetime):
        output_path = self._uris.gp2gp_transfers(daily_start_datetime=daily_start_datetime)
        self._io.write_transfers(transfers, output_path)

    def run(self):
        transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)

        for day in self._reporting_window.get_dates():
            spine_messages = self._read_spine_messages()

            transfers = parse_transfers_from_messages(
                spine_messages=spine_messages,
                conversation_cutoff=self._cutoff,
                daily_start_datetime=day,
                observability_probe=transfer_observability_probe,
            )
            self._write_transfers(transfers=transfers, daily_start_datetime=day)


def main():
    _setup_logger()
    config = TransferClassifierConfig.from_environment_variables(environ)
    TransferClassifierPipeline(
        config
    ).run() if config.date_anchor is None else TransferClassifierMonthlyPipeline(config).run()


if __name__ == "__main__":
    main()
