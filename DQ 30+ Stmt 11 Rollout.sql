select
    date_trunc('quarter',a.evaluated_timestamp)::date as clip_yq
    ,b.statement_num - 11 as statement_num_post_clip
    ,sum(b.delinquency_d030_stmt_usd) as DQ30_usd
    ,case when sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) <> 0 then sum((b.delinquency_d030_stmt_usd)+(b.delinquency_d060_stmt_usd)+(b.delinquency_d090_stmt_usd)+(b.delinquency_d120_stmt_usd)+(b.delinquency_d150_stmt_usd)) / sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD)
         else 0
         end as DQ30_plus_USD
    ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as OS
from (select * from edw_db.public.clip_results_data where statement_number = 11 
        and ((test_segment ilike '%rollout%' and outcome ilike '%approved%') or (date_trunc('quarter',evaluated_timestamp) <= '2020-10-01' and outcome ilike '%approved%'))) a
    left join edw_db.public.account_statements b
        on a.card_id = b.account_id
where b.statement_num >= 11
group by 1,2
