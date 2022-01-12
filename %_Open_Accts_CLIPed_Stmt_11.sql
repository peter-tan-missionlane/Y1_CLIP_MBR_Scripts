//// % Open Accounts CLIP'd at statement 11 (excludes Q2 2021 fraud attack population)

select
    date_trunc('month',evaluated_timestamp)::date as clip_month,
    sum(case when outcome ilike '%approve%' then 1 else 0 end) as Approved_CLIP7,
    count(distinct card_id) as Total_Accts,
    Approved_CLIP7/Total_accts as CLIP7_rate_nonfraud
from (select * from edw_db.public.clip_results_data where statement_number = 11 and date_trunc('month',evaluated_timestamp) >= '2021-01-01') a 
    inner join
      (select * 
          from edw_db.Public.account_statements
          where statement_num = 11
                  and customer_id not in (select user_id as customer_id from sandbox_db.user_tb.BWEISS_FRAUD_ATTACK_MAY_24)) b
     on a.card_id = b.account_id
group by 1
