from prmdata.domain.gp2gp.national_metrics import NationalMetrics, IntegratedMetrics
from tests.builders.common import an_integer


def test_calculates_paper_fallback_count_given_all_transfers_within_sla():
    national_metrics = NationalMetrics(
        initiated_transfer_count=2,
        pending_transfer_count=an_integer(),
        failed_transfer_count=an_integer(),
        integrated=IntegratedMetrics(
            transfer_count=an_integer(),
            within_3_days=1,
            within_8_days=1,
            beyond_8_days=an_integer(),
        ),
    )
    expected_paper_fallback_count = 0

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count


def test_calculates_paper_fallback_count_given_one_transfer_beyond_sla():
    national_metrics = NationalMetrics(
        initiated_transfer_count=2,
        pending_transfer_count=an_integer(),
        failed_transfer_count=an_integer(),
        integrated=IntegratedMetrics(
            transfer_count=an_integer(),
            within_3_days=1,
            within_8_days=0,
            beyond_8_days=an_integer(),
        ),
    )
    expected_paper_fallback_count = 1

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count


def test_calculates_paper_fallback_count_given_unsuccessful_and_late_integrations():
    national_metrics = NationalMetrics(
        initiated_transfer_count=5,
        pending_transfer_count=an_integer(),
        failed_transfer_count=an_integer(),
        integrated=IntegratedMetrics(
            transfer_count=an_integer(),
            within_3_days=1,
            within_8_days=0,
            beyond_8_days=an_integer(),
        ),
    )
    expected_paper_fallback_count = 4

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count
