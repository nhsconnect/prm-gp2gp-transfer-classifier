import logging
from os import environ
from typing import List

import boto3

from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierS3UriResolver
from prmdata.pipeline.parse_transfers_from_messages import parse_transfers_from_messages
from prmdata.utils.io.json_formatter import JsonFormatter
from prmdata.utils.io.s3 import S3DataManager

logger = logging.getLogger("prmdata")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class TransferClassifierPipeline:
    def __init__(self, config: TransferClassifierConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)

        self._cutoff = config.conversation_cutoff

        self._uris = TransferClassifierS3UriResolver(
            gp2gp_spine_bucket=config.input_spine_data_bucket,
            transfers_bucket=config.output_transfer_data_bucket,
        )

        self._spine_data_uris = config.spine_data_s3_uris

        output_metadata = {
            "date-anchor": config.date_anchor.isoformat(),
            "cutoff-days": str(config.conversation_cutoff.days),
            "build-tag": config.build_tag,
        }

        self._io = TransferClassifierIO(s3_manager, output_metadata)

    def _read_spine_messages_deprecated(self, metric_month, overflow_month):
        input_path = self._uris.spine_messages(metric_month, overflow_month)
        return self._io.read_spine_messages(input_path)

    def _read_spine_messages(self, spine_data_uris: List[str]):
        return self._io.read_spine_messages(s3_uris=spine_data_uris)

    def _write_transfers(self, transfers, metric_month):
        output_path = self._uris.gp2gp_transfers(metric_month)
        self._io.write_transfers(transfers, output_path)

    def run(self):
        metric_month = self._reporting_window.metric_month
        overflow_month = self._reporting_window.overflow_month
        if self._spine_data_uris is None:
            spine_messages = self._read_spine_messages_deprecated(metric_month, overflow_month)
        else:
            spine_messages = self._read_spine_messages(self._spine_data_uris)

        transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
        transfers = parse_transfers_from_messages(
            spine_messages=spine_messages,
            reporting_window=self._reporting_window,
            conversation_cutoff=self._cutoff,
            observability_probe=transfer_observability_probe,
        )
        self._write_transfers(transfers, metric_month)


def main():
    _setup_logger()
    config = TransferClassifierConfig.from_environment_variables(environ)
    pipeline = TransferClassifierPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
