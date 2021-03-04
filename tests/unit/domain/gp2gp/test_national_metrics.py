from prmdata.domain.gp2gp.national_metrics import NationalMetrics, IntegratedMetrics


def test_calculates_paper_fallback_count_given_all_transfers_within_sla():
    national_metrics = NationalMetrics(
        initiated_transfers_count=2,
        failed_transfers_count=0,
        integrated=IntegratedMetrics(
            transfer_count=2, within_3_days=1, within_8_days=1, beyond_8_days=0
        ),
    )
    expected_paper_fallback_count = 0

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count


def test_calculates_paper_fallback_count_given_one_transfer_beyond_sla():
    national_metrics = NationalMetrics(
        initiated_transfers_count=2,
        failed_transfers_count=0,
        integrated=IntegratedMetrics(
            transfer_count=2, within_3_days=1, within_8_days=0, beyond_8_days=1
        ),
    )
    expected_paper_fallback_count = 1

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count


def test_calculates_paper_fallback_count_given_unsuccessful_and_late_integrations():
    national_metrics = NationalMetrics(
        initiated_transfers_count=5,
        failed_transfers_count=3,
        integrated=IntegratedMetrics(
            transfer_count=2, within_3_days=1, within_8_days=0, beyond_8_days=1
        ),
    )
    expected_paper_fallback_count = 4

    assert national_metrics.calculate_paper_fallback() == expected_paper_fallback_count
