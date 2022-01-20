import boto3

from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.domain.monthly_reporting_window import MonthlyReportingWindow
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierMonthlyS3UriResolver
from prmdata.pipeline.parse_transfers_from_messages import parse_transfers_from_messages_monthly
from prmdata.utils.input_output.s3 import S3DataManager


class TransferClassifierMonthly:
    def __init__(self, config: TransferClassifierConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)

        self._cutoff = config.conversation_cutoff

        self._uris = TransferClassifierMonthlyS3UriResolver(
            gp2gp_spine_bucket=config.input_spine_data_bucket,
            transfers_bucket=config.output_transfer_data_bucket,
        )

        self._metadata = {
            "date-anchor": config.date_anchor.isoformat(),
            "cutoff-days": str(config.conversation_cutoff.days),
            "build-tag": config.build_tag,
        }

        self._io = TransferClassifierIO(s3_manager)

    def _read_spine_messages(self, metric_month, overflow_month):
        input_path = self._uris.spine_messages(metric_month, overflow_month)
        return self._io.read_spine_messages(input_path)

    def _write_transfers(self, transfers, metric_month):
        output_path = self._uris.gp2gp_transfers(metric_month)
        self._io.write_transfers(transfers, output_path, self._metadata)

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
