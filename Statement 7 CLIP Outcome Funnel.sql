with funnel_drv as(
select
   card_id
  ,b.account_id
  ,b.customer_id
  ,statement_number
  ,substring(b.statement_end_dt,1,7) as stmt_month
  ,to_char(EVALUATED_TIMESTAMP,'YYYY-MM') AS month_evaluated
  ,credit_limit_orig_usd as ICL
  ,outcome
  ,DECISION_DATA
  ,CLIP_RISK_GROUP
  ,CLIP_POLICY_NAME
  ,TEST_SEGMENT
  ,(POST_CLIP_LINE_LIMIT - PRE_CLIP_LINE_LIMIT) as CLIP_AMT
  ,decision_data:"never_delinquent__passed" as no_DQ_flag1
  ,decision_data:"delinquency__passed" as no_DQ_flag2
  ,COALESCE(no_DQ_flag1, no_DQ_flag2) AS no_DQ_Flag
  ,decision_data:"average_utilization_3_months"::FLOAT as util_at_clip1 
  ,decision_data:"average-credit-line-utilization"::FLOAT as util_at_clip2
  ,decision_data:"average_credit_line_utilization_last_3_statements"::FLOAT as util_at_clip3
  ,decision_data:"current_principal_utilization"::FLOAT as util_at_clip4
  ,decision_data:"internal__fis_tsys__PRINCIPAL_UTILIZATION"::FLOAT as util_at_clip5
  ,COALESCE(util_at_clip1,util_at_clip2,util_at_clip3,util_at_clip4,util_at_clip5) as util_at_clip
  ,case when util_at_clip < 0.1  then 'A.<10%'
      when util_at_clip <0.3  then 'B.10%-30%'
      when util_at_clip <.5  then 'C.30%-50%'
      when util_at_clip <0.8  then 'D.50%-80%'
      when util_at_clip >=0.8  then 'E.>80%'
      end as util_band
from EDW_DB.PUBLIC.CLIP_RESULTS_DATA a
  inner join
      (select * 
          from edw_db.Public.account_statements
          where statement_num = 7
                  and customer_id not in (select user_id as customer_id from sandbox_db.user_tb.BWEISS_FRAUD_ATTACK_MAY_24)) b
     on a.card_id = b.account_id
)


///Statement 7/11 funnel
select
    month_evaluated as clip_month
    ,outcome
    ,case when outcome ilike '%approved%' and clip_amt > 100 then 'A) Approved, >minCLIP'
          when outcome ilike '%approved%' and clip_amt = 100 and util_band ilike '%A.%' then 'B) Approved, minCLIP, Low Util'
          when outcome ilike '%approved%' and clip_amt = 100 then 'C) Approved, minCLIP, High Risk'
          when outcome ilike '%ineligible%' and (no_dq_flag ilike '%false%') then 'D) Ineligible - DQ Cut'
          when outcome ilike '%ineligible%'                                  then 'E) Ineligible - Hardcut'
          when outcome ilike '%declined%' and (util_at_clip < 0.1)           then 'F) Declined - Low Util'
          when outcome ilike '%declined%'                                    then 'G) Declined - High Risk'
     end as CLIP_outcome_group
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
      NO_DQ_FLAG,
      util_band,
      count(card_id) as accounts,
      sum(clip_amt)
    from funnel_drv
    group by 1,2,3,4,5,6,7,8,9,10,11)
where
    statement_number = 7
group by 1,2,3
