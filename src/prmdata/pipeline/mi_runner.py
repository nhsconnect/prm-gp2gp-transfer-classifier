import logging
from typing import List

from prmdata.domain.mi.mi_service import MiService
from prmdata.pipeline.transfer_classifier import TransferClassifier

logger = logging.getLogger(__name__)


class MiRunner(TransferClassifier):
    def _read_mi_events(self) -> List[dict]:
        input_paths = self._uris.mi_events(self._reporting_window)
        return self._io.read_json_files_from_paths(input_paths)

    def run(self):
        self._runner_observability_probe.log_attempting_to_classify()

        mi_events = self._read_mi_events()
        self._runner_observability_probe.log_successfully_read_mi_events(mi_events)

        mi_service = MiService()

        mi_messages = mi_service.construct_mi_messages_from_mi_events(mi_events)
        self._runner_observability_probe.log_successfully_constructed_mi_messages(mi_messages)

        grouped_mi_messages = mi_service.group_mi_messages_by_conversation_id(mi_messages)
        # self._runner_observability_probe.log_successfully_grouped_mi_messages(grouped_mi_messages)

        transfers = mi_service.convert_to_mi_transfers(grouped_mi_messages)
        self._runner_observability_probe.log_successfully_created_transfers_from_mi_events(
            transfers
        )

        return transfers
