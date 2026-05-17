from typing import List, Tuple


def clamp_score(score: float) -> int:
    return max(0, min(100, int(round(score))))


def to_risk_band(score: int) -> str:
    if score >= 85:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def compute_risk_score(
    amount: float,
    avg_amount_30m: float,
    tx_count_5m: int,
    failed_count_5m: int,
    is_new_country: bool,
    is_new_device: bool,
    is_international: bool,
) -> Tuple[int, str, List[str]]:
    score = 0.0
    reasons: List[str] = []

    if avg_amount_30m > 0 and amount > avg_amount_30m * 3:
        score += 30
        reasons.append("amount_spike_vs_user_baseline")
    elif amount >= 2000:
        score += 20
        reasons.append("high_transaction_amount")

    if tx_count_5m >= 6:
        score += 25
        reasons.append("high_velocity_transactions")

    if failed_count_5m >= 3:
        score += 20
        reasons.append("failed_payment_burst")

    if is_new_country and amount >= 300:
        score += 15
        reasons.append("new_country_with_nontrivial_amount")

    if is_new_device and amount >= 300:
        score += 10
        reasons.append("new_device_with_nontrivial_amount")

    if is_international and amount >= 500:
        score += 10
        reasons.append("international_payment_risk")

    final_score = clamp_score(score)
    return final_score, to_risk_band(final_score), reasons

