from _warnings import warn
from typing import NamedTuple, Iterable, Iterator

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.service.transfer import Transfer
from gp2gp.service.common import assign_to_sla_band, SlaBand


class PracticeSlaMetrics(NamedTuple):
    ods_code: str
    name: str
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


def calculate_sla_by_practice(
    practice_list: Iterable[PracticeDetails], transfers: Iterable[Transfer]
) -> Iterator[PracticeSlaMetrics]:
    default_sla = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}
    practice_counts = {practice.ods_code: default_sla.copy() for practice in practice_list}

    asid_to_ods_mapping = {
        asid: practice.ods_code for practice in practice_list for asid in practice.asids
    }

    unexpected_asids = set()
    for transfer in transfers:
        asid = transfer.requesting_practice_asid

        if asid in asid_to_ods_mapping:
            ods_code = asid_to_ods_mapping[asid]
            sla_band = assign_to_sla_band(transfer.sla_duration)
            practice_counts[ods_code][sla_band] += 1
        else:
            unexpected_asids.add(asid)

    if len(unexpected_asids) > 0:
        warn(f"Unexpected ASID count: {len(unexpected_asids)}", RuntimeWarning)

    return (
        PracticeSlaMetrics(
            practice.ods_code,
            practice.name,
            within_3_days=practice_counts[practice.ods_code][SlaBand.WITHIN_3_DAYS],
            within_8_days=practice_counts[practice.ods_code][SlaBand.WITHIN_8_DAYS],
            beyond_8_days=practice_counts[practice.ods_code][SlaBand.BEYOND_8_DAYS],
        )
        for practice in practice_list
    )
