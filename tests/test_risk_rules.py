from src.common.risk_rules import compute_risk_score


def test_high_risk_transaction():
    score, band, reasons = compute_risk_score(
        amount=3000,
        avg_amount_30m=500,
        tx_count_5m=7,
        failed_count_5m=4,
        is_new_country=True,
        is_new_device=True,
        is_international=True,
    )
    assert score >= 85
    assert band == "critical"
    assert "amount_spike_vs_user_baseline" in reasons
    assert "high_velocity_transactions" in reasons


def test_low_risk_transaction():
    score, band, reasons = compute_risk_score(
        amount=30,
        avg_amount_30m=40,
        tx_count_5m=1,
        failed_count_5m=0,
        is_new_country=False,
        is_new_device=False,
        is_international=False,
    )
    assert score < 40
    assert band == "low"
    assert reasons == []

