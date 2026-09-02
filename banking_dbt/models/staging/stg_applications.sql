select
    sk_id_curr,
    amt_income_total,
    amt_credit,
    amt_annuity,
    total_credit_sum,
    total_credit_sum_debt,
    active_credits_count,
    total_credits_count,
    credit_to_income_ratio,
    debt_to_income_ratio,
    is_high_risk
from {{ source('raw', 'raw_enriched_applications') }}