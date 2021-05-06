from warnings import warn
from typing import NamedTuple, Iterable, Iterator

from prmdata.domain.ods_portal.models import PracticeDetails
from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.sla import SlaCounter


class IntegratedPracticeMetrics(NamedTuple):
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


class PracticeMetrics(NamedTuple):
    ods_code: str
    name: str
    integrated: IntegratedPracticeMetrics


def _derive_practice_sla_metrics(practice, sla_metrics):
    return PracticeMetrics(
        practice.ods_code,
        practice.name,
        integrated=IntegratedPracticeMetrics(
            transfer_count=sla_metrics.total(),
            within_3_days=sla_metrics.within_3_days(),
            within_8_days=sla_metrics.within_8_days(),
            beyond_8_days=sla_metrics.beyond_8_days(),
        ),
    )


def calculate_sla_by_practice(
    practice_list: Iterable[PracticeDetails], transfers: Iterable[Transfer]
) -> Iterator[PracticeMetrics]:
    practice_counts = {practice.ods_code: SlaCounter() for practice in practice_list}

    asid_to_ods_mapping = {
        asid: practice.ods_code for practice in practice_list for asid in practice.asids
    }

    _process_asid(asid_to_ods_mapping, practice_counts, transfers)

    return (
        _derive_practice_sla_metrics(practice, practice_counts[practice.ods_code])
        for practice in practice_list
    )


def _process_asid(asid_to_ods_mapping, practice_counts, transfers):
    unexpected_asids = set()
    for transfer in transfers:
        asid = transfer.requesting_practice_asid

        if asid in asid_to_ods_mapping:
            ods_code = asid_to_ods_mapping[asid]
            practice_counts[ods_code].increment(transfer.sla_duration)
        else:
            unexpected_asids.add(asid)
    if len(unexpected_asids) > 0:
        warn(f"Unexpected ASID count: {len(unexpected_asids)}", RuntimeWarning)
