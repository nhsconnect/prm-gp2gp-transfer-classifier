import logging
from typing import Iterator

from prmdata.domain.spine.message import Message
from prmdata.pipeline.transfer_classifier import TransferClassifier

logger = logging.getLogger(__name__)


class MiRunner(TransferClassifier):
    def _read_mi_messages(self) -> Iterator[Message]:
        input_paths = self._uris.mi_events(self._reporting_window)
        return self._io.read_json_files_from_paths(input_paths)

    def run(self):
        self._runner_observability_probe.log_attempting_to_classify()

        logger.info(self._read_mi_messages())

        self._runner_observability_probe.log_successfully_classified(
            ods_metadata_input_paths=self._ods_metadata_input_paths
        )
