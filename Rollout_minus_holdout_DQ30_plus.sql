create or replace temporary table CLIP11_DQ_RO as
select
    date_trunc('quarter',a.evaluated_timestamp)::date as clip_yq
    ,CONCAT(cast(year(a.evaluated_timestamp) as char(4)), 'Q', cast(quarter(a.evaluated_timestamp) as char(1))) as CLIP_Q
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
group by 1,2,3
;

create or replace temporary table CLIP11_DQ_HO as
select
    date_trunc('quarter',a.evaluated_timestamp)::date as clip_yq
    ,CONCAT(cast(year(a.evaluated_timestamp) as char(4)), 'Q', cast(quarter(a.evaluated_timestamp) as char(1))) as CLIP_Q
    ,b.statement_num - 11 as statement_num_post_clip
    ,sum(b.delinquency_d030_stmt_usd) as DQ30_usd
    ,case when sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) <> 0 then sum((b.delinquency_d030_stmt_usd)+(b.delinquency_d060_stmt_usd)+(b.delinquency_d090_stmt_usd)+(b.delinquency_d120_stmt_usd)+(b.delinquency_d150_stmt_usd)) / sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD)
         else 0
         end as DQ30_plus_USD
    ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as OS
    //,count(distinct b.account_id)
from (select * from edw_db.public.clip_results_data where statement_number = 11 
        and test_segment ilike '%hold%') a
    left join edw_db.public.account_statements b
        on a.card_id = b.account_id
where b.statement_num >= 11
group by 1,2,3
;

select
    a.clip_q
    ,a.statement_num_post_clip
    ,a.dq30_plus_usd as RO_dq30_plus_usd
    ,b.dq30_plus_usd as HO_dq30_plus_usd
    ,RO_dq30_plus_usd - HO_dq30_plus_usd as RO_minus_HO_dq30_plus_usd
    ,case when HO_dq30_plus_usd = 0 then 0 else (RO_dq30_plus_usd/HO_dq30_plus_usd)-1 end as RO_minus_HO_minus_1_dq30_plus_usd
from clip11_dq_RO a
    join clip11_dq_HO b
        on a.clip_q = b.clip_q
            and a.statement_num_post_clip = b.statement_num_post_clip
