///Average CLIP per CLIP'd account at statement 11

select
    date_trunc('month',evaluated_timestamp)::date as clip_month
    ,avg(clip_risk_group)
    ,avg(post_clip_line_limit) as post_clip_line_avg
    ,avg(pre_clip_line_limit) as pre_clip_line_avg
    ,post_clip_line_avg - pre_clip_line_avg as clip_11_amt
from edw_db.public.clip_results_data
where statement_number = 11
    and outcome ilike '%approved%'
group by 1
;
