with cte_main
as
(select step.LP_STEP_NAME,step.IND_ENABLED,step.PAR_I_LP_STEP,step.I_LP_STEP
from snp_load_plan lp
    inner join snp_lp_step  step
        on lp.i_load_plan = step.i_load_plan
        --and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name = 'LP_JDEGS_INCREMENTAL')
select CONCAT
   (
      LPAD
      (
         ' ',
         LEVEL*3-3
      ),
      cte_main.LP_STEP_NAME
   ) stp
from cte_main
connect by 
    prior cte_main.I_LP_STEP = cte_main.PAR_I_LP_STEP
start with 
    cte_main.PAR_I_LP_STEP is null
;


---Recursive SQL to get all the steps whose parent step has been disabled
with lp_stp(LP_STEP_NAME,IND_ENABLED,PAR_I_LP_STEP,I_LP_STEP,LP_STEP_TYPE,i_load_plan)
as
(select step.LP_STEP_NAME,step.IND_ENABLED,step.PAR_I_LP_STEP,step.I_LP_STEP,step.LP_STEP_TYPE,step.i_load_plan
    from snp_load_plan lp
        inner join snp_lp_step  step
            on lp.i_load_plan = step.i_load_plan
            --and step.LP_STEP_TYPE = 'RS'
    where lp.load_plan_name = 'LP_JDEGS_INCREMENTAL'
    and step.ind_enabled = 0
 union all
 select cstep.LP_STEP_NAME,cstep.IND_ENABLED,cstep.PAR_I_LP_STEP,cstep.I_LP_STEP,cstep.LP_STEP_TYPE,cstep.i_load_plan
 from snp_lp_step  cstep
    inner join lp_stp
        on cstep.i_load_plan = lp_stp.i_load_plan
        and cstep.par_i_lp_step = lp_stp.I_LP_STEP
)
select lp_step_name from lp_stp
where lp_step_type = 'RS'
;

--All the LP steps that are disabled---
with lp
as
(select distinct slp.load_plan_name,slp.i_load_plan
from SNP_LP_INST INST
    inner join SNP_LOAD_PLAN slp
        on INST.load_plan_name = slp.load_plan_name
    inner join snp_lp_step sls
        on slp.i_load_plan = sls.i_load_plan
        and sls.LP_STEP_NAME = 'root_step'
where 1=1
and sls.IND_ENABLED =1
and slp.load_plan_name not in ('LP_JDEGS_UBE_6:30PM_DAILY','LP_JDEGS_UBE_SAT_9AM','LP_JDEGS_UBE_SUN_6AM','LP_JDEGS_UBE_DAILY'))
,
dis_lp_stp(LP_STEP_NAME,IND_ENABLED,PAR_I_LP_STEP,I_LP_STEP,LP_STEP_TYPE,i_load_plan)
as
(select step.LP_STEP_NAME,step.IND_ENABLED,step.PAR_I_LP_STEP,step.I_LP_STEP,step.LP_STEP_TYPE,step.i_load_plan
    from snp_load_plan lp
        inner join snp_lp_step  step
            on lp.i_load_plan = step.i_load_plan
            --and step.LP_STEP_TYPE = 'RS'
    where 1=1---lp.load_plan_name = 'LP_JDEGS_INCREMENTAL'
    and step.ind_enabled = 0
 union all
 select cstep.LP_STEP_NAME,cstep.IND_ENABLED,cstep.PAR_I_LP_STEP,cstep.I_LP_STEP,cstep.LP_STEP_TYPE,cstep.i_load_plan
 from snp_lp_step  cstep
    inner join dis_lp_stp
        on cstep.i_load_plan = dis_lp_stp.i_load_plan
        and cstep.par_i_lp_step = dis_lp_stp.I_LP_STEP
)
select lp.load_plan_name, dstp.lp_step_name 
from dis_lp_stp  dstp
    inner join lp
        on lp.i_load_plan = dstp.i_load_plan
where lp_step_type = 'RS'
;


