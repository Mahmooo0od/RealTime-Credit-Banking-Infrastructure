select
    sk_id_curr,
    amt_income_total,
    amt_credit,
    amt_annuity,
    credit_to_income_ratio,
    debt_to_income_ratio,
    is_high_risk,
    current_timestamp as loaded_at

from {{ ref('stg_applications') }}