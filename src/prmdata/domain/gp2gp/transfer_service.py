from typing import Iterator

from prmdata.domain.gp2gp.transfer import derive_transfer, TransferObservabilityProbe
from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation


class TransferService:
    _probe = TransferObservabilityProbe()

    def convert_to_transfers(self, conversations: Iterator[Gp2gpConversation]):
        return (derive_transfer(conversation, self._probe) for conversation in conversations)
