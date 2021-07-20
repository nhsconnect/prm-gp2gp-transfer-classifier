from warnings import warn
from typing import NamedTuple, Iterable, Iterator

from prmdata.domain.gp2gp.practice_lookup import PracticeLookup
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
    practice_lookup: PracticeLookup, transfers: Iterable[Transfer]
) -> Iterator[PracticeMetrics]:
    practice_counts = {ods_code: SlaCounter() for ods_code in practice_lookup.all_ods_codes()}

    unexpected_asids = set()
    for transfer in transfers:
        asid = transfer.requesting_practice.asid

        if practice_lookup.has_asid_code(asid):
            ods_code = practice_lookup.ods_code_from_asid(asid)
            practice_counts[ods_code].increment(transfer.sla_duration)
        else:
            unexpected_asids.add(asid)

    if len(unexpected_asids) > 0:
        warn(f"Unexpected ASID count: {len(unexpected_asids)}", RuntimeWarning)

    return (
        _derive_practice_sla_metrics(practice, practice_counts[practice.ods_code])
        for practice in practice_lookup.all_practices()
    )
