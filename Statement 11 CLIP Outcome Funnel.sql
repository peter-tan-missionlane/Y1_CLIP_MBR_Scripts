with funnel_drv as(
select
   card_id
  ,statement_number
  ,substring(b.statement_end_dt,1,7) as stmt_month
  ,to_char(EVALUATED_TIMESTAMP,'YYYY-MM') AS month_evaluated
  ,outcome
  ,DECISION_DATA
  ,CLIP_RISK_GROUP
  ,CLIP_POLICY_NAME
  ,TEST_SEGMENT
  ,(POST_CLIP_LINE_LIMIT - PRE_CLIP_LINE_LIMIT) as CLIP_AMT
  ,decision_data:"never_delinquent__passed" as no_DQ_flag1
  ,decision_data:"delinquency__passed" as no_DQ_flag2
  ,decision_data:"average_utilization_3_months"::FLOAT as util_at_clip 
from EDW_DB.PUBLIC.CLIP_RESULTS_DATA a
  inner join
      (select * 
          from edw_db.Public.account_statements
          where statement_num = 11
                  and customer_id not in (select user_id as customer_id from sandbox_db.user_tb.BWEISS_FRAUD_ATTACK_MAY_24)) b
     on a.card_id = b.account_id
)


///Statement 7/11 funnel
select
    month_evaluated as clip_month
    ,outcome
    ,case when outcome ilike '%approved%' then 'Approved'
          when outcome ilike '%ineligible%' and (no_dq_flag1 ilike '%false%' or no_dq_flag2 ilike '%false%') then 'Ineligible - DQ Cut'
          when outcome ilike '%ineligible%'                                  then 'Ineligible - Hardcut'
          when outcome ilike '%declined%' and (util_at_clip < 0.1)           then 'Declined - Low Util'
          when outcome ilike '%declined%'                                    then 'Declined - High Risk'
     end as CLIP_outcome_group
    ,case when clip_amt > 100 then '>$100'
          when clip_amt = 100 and util_band not ilike '%A.%' then '$100 - Util > 10%'
          when clip_amt = 100 then '$100 - Util < 10%'
     end as CLIP_amt_group
    ,sum(accounts)
from
    (select 
      stmt_month,
      statement_number,
      month_evaluated,
      outcome,
      CLIP_RISK_GROUP,
      CLIP_POLICY_NAME,
      TEST_SEGMENT,
      CLIP_AMT,
      UTIL_AT_CLIP,
      NO_DQ_FLAG1,
      NO_DQ_FLAG2,
      case when util_at_clip < 0.1 then 'A.<10%'
      when util_at_clip <0.3 then 'B.10%-30%'
      when util_at_clip <.5 then 'C.30%-50%'
      when util_at_clip <0.8 then 'D.50%-80%'
      when util_at_clip >=0.8  then 'E.>80%'
      end as util_band,
      count(card_id) as accounts,
      sum(clip_amt)
    from funnel_drv
    group by 1,2,3,4,5,6,7,8,9,10,11)
where
    statement_number = 11
group by 1,2,3,4
