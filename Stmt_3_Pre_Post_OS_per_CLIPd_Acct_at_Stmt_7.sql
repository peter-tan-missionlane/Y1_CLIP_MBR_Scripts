//Statement 3 Pre-Post OS/CLIped Acct and $DQ30-60% (post statement 7 CLIP)  

create or replace temporary table CLIP7_accounts as
select
    card_id
    ,date_trunc('month',evaluated_timestamp)::date as clip_month
    ,post_clip_line_limit
    ,pre_clip_line_limit
    ,post_clip_line_limit - pre_clip_line_limit as clip__amt
    ,test_segment
    ,outcome
    ,b.AVG_OUTSTANDING_BALANCE_STMT_USD as clipd_stmt_os_bal
from (select * from edw_db.public.clip_results_data where statement_number = 7 and outcome ilike '%approved%') a
    left join EDW_DB.PUBLIC.ACCOUNT_STATEMENTS b
        on a.card_id = b.account_id
            and a.statement_number = b.statement_num
;
select 
      //clip_month as clip_stmt7_month
      date_trunc('month',statement_end_dt)::date as stmt10_month
      ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD) as rollout_stmt3_OS
      ,count(distinct a.card_id) as rollout_accts
      ,sum(b.AVG_OUTSTANDING_BALANCE_STMT_USD)/count(distinct a.card_id) as rollout_stmt3_OS_per_clip_acct
      ,sum(a.clipd_stmt_os_bal) as pre_clip_os
      ,pre_clip_os / count(distinct a.card_id) as pre_clip_os_per_clip_acct
      ,rollout_stmt3_OS_per_clip_acct-pre_clip_os_per_clip_acct as pre_post_incremental_os
  from CLIP7_accounts a
      left join EDW_DB.PUBLIC.ACCOUNT_STATEMENTS b
          on a.card_id = b.account_id
  where b.statement_num = 10
  group by 1
