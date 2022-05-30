from datetime import timedelta
from typing import Iterator

from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_day
from prmdata.domain.spine.message import Message
from prmdata.pipeline.transfer_classifier import TransferClassifier
from prmdata.utils.date_converter import convert_to_datetime_string


class SpineRunner(TransferClassifier):
    def _read_spine_messages(self) -> Iterator[Message]:
        input_paths = self._uris.spine_messages(self._reporting_window)
        return self._io.read_spine_messages(input_paths)

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
