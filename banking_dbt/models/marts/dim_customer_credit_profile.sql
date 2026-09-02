select distinct
    sk_id_curr,
    total_credit_sum,
    total_credit_sum_debt,
    active_credits_count,
    total_credits_count,
    case
        when total_credits_count = 0 then 'No Credit History'
        when active_credits_count = 0 then 'Fully Settled'
        when active_credits_count > 0 and total_credit_sum_debt > total_credit_sum * 0.8 then 'High Utilization'
        else 'Normal'
    end as credit_profile_segment

from {{ ref('stg_applications') }}