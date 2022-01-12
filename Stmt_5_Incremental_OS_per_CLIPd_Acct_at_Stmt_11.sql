//Statement 5 Incremental OS/CLIped Acct (post statement 11 CLIP) 

create or replace temporary table CLIP_rollout as
select
    card_id
    ,date_trunc('month',evaluated_timestamp)::date as clip_month
    ,post_clip_line_limit
    ,pre_clip_line_limit
    ,post_clip_line_limit - pre_clip_line_limit as clip__amt
    ,test_segment
from edw_db.public.clip_results_data
where statement_number = 11
    and test_segment ilike '%rollout%'
    and outcome ilike '%approved%'
;

create or replace temporary table CLIP_holdout as
select
    card_id
    ,date_trunc('month',evaluated_timestamp)::date as clip_month
    ,post_clip_line_limit
    ,pre_clip_line_limit
    ,post_clip_line_limit - pre_clip_line_limit as clip__amt 
    ,outcome
    ,test_segment
    //distinct test_segment, count(*)
from edw_db.public.clip_results_data
where statement_number = 11
    and test_segment ilike '%hold%'
    //and outcome ilike '%approved%'
    //and evaluated_timestamp ilike '%2021-07%'
//group by 1
;

select
    a.stmt16_month
    ,(rollout_stmt5_OS - holdout_stmt5_OS) as incremental_stmt5_OS
    ,rollout_stmt5_OS_per_clip_acct - holdout_stmt5_OS_per_clip_acct as incremental_stmt5_OS_per_clip_acct
from
  (select 
      //clip_month
      date_trunc('month',statement_end_dt)::date as stmt16_month
      //,concat(year(a.clip_month),'Q',quarter(a.clip_month)) as clip_yq
      ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as rollout_stmt5_OS
      ,count(distinct a.card_id) as rollout_accts
      ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD)/count(distinct a.card_id) as rollout_stmt5_OS_per_clip_acct
      ,sum(b.delinquency_d030_stmt_usd)-sum(b.delinquency_d060_stmt_usd) as DQ30_60_usd
      ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as OS
  from CLIP_rollout a
      left join EDW_DB.PUBLIC.ACCOUNT_STATEMENTS b
          on a.card_id = b.account_id
  where b.statement_num = 16
  group by 1) a
  join
    (select 
        //clip_month
        date_trunc('month',statement_end_dt)::date as stmt16_month
        //,concat(year(clip_month),'Q',quarter(clip_month)) as clip_yq
        ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as holdout_stmt5_OS
        ,count(distinct a.card_id) as holdout_accts
        ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD)/count(distinct a.card_id) as holdout_stmt5_OS_per_clip_acct
        ,sum(b.delinquency_d030_stmt_usd)-sum(b.delinquency_d060_stmt_usd) as DQ30_60_usd
        ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as OS
    from CLIP_holdout a
        left join EDW_DB.PUBLIC.ACCOUNT_STATEMENTS b
            on a.card_id = b.account_id
    where b.statement_num = 16
    group by 1) b
        on a.stmt16_month = b.stmt16_month
