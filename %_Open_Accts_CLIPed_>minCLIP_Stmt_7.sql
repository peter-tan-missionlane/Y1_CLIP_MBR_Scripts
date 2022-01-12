//// % Open Accounts CLIP'd at statement 7 over minCLIP (excludes Q2 2021 fraud attack population)
select
    date_trunc('month',evaluated_timestamp)::date as clip_month,
    sum(case when outcome ilike '%approve%' and post_clip_line_limit-100>pre_clip_line_limit then 1 else 0 end) as CLIP7_aboveMinCLIP,
    count(distinct card_id) as Total_Accts,
    CLIP7_aboveMinCLIP/Total_accts as CLIP7_aboveminCLIP_rate_nonfraud
from (select * from edw_db.public.clip_results_data where statement_number = 7 and date_trunc('month',evaluated_timestamp) >= '2021-09-01') a 
    inner join
      (select * 
          from edw_db.Public.account_statements
          where statement_num = 7
                  and customer_id not in (select user_id as customer_id from sandbox_db.user_tb.BWEISS_FRAUD_ATTACK_MAY_24)) b
     on a.card_id = b.account_id
group by 1
