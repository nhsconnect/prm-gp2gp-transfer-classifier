import logging
from typing import List

from prmdata.domain.mi.mi_service import MiService
from prmdata.pipeline.transfer_classifier import TransferClassifier

logger = logging.getLogger(__name__)


class MiRunner(TransferClassifier):
    def _read_mi_events(self) -> List[dict]:
        input_paths = self._uris.mi_events(self._reporting_window)
        logger.info(
            {"event": "ATTEMPTING_TO_READ_MI_EVENTS_FROM_PATHS", "input_paths": input_paths}
        )
        return self._io.read_json_files_from_paths(input_paths)

    def run(self):
        self._runner_observability_probe.log_attempting_to_classify()

        mi_events = self._read_mi_events()
        logger.info({"event": "SUCCESSFULLY_READ_MI_EVENTS", "events": mi_events})

        mi_messages = MiService.construct_mi_messages_from_mi_events(mi_events)
        self._runner_observability_probe.log_successfully_constructed_mi_messages(mi_messages)
