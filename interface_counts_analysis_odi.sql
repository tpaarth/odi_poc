--old query
select listagg(LP_SCEN,',') within group (order by lp_scen)
from 
(with cte_lp
as
(SELECT
    scen_name     AS LP_SCEN,
    scen_version  AS VERSION,
    time,
    frequency,
    LISTAGG(load_sch.elmnt, ',') WITHIN GROUP(
        ORDER BY
            load_sch.scen_name
    )             AS day
FROM
    (
        SELECT
            a.scen_name,
            a.scen_version,
            to_char(to_date(a.s_hour
                            || ':'
                            || a.s_minute
                            || ':'
                            || a.s_second, 'HH24:MI:SS'), 'HH:MI:SS AM')            AS time,
            decode(s_type, 'H', 'Hourly', 'D', 'Daily',
                   'W', 'Weekly')                                 AS frequency,
            decode(TRIM(x.column_value.extract('e/text()')), '1', 'Sunday', '2', 'Monday',
                   '3', 'Tuesday', '4', 'Wednesday', '5',
                   'Thursday',
                   '6',
                   'Friday',
                   '7',
                   'Saturday')                                   AS elmnt
        FROM
                 snp_plan_agent a
            JOIN TABLE ( xmlsequence(xmltype('<e><e>'
                                             || replace(a.s_week_day, ',', '</e><e>')
                                             || '</e></e>').extract('e/e')) ) x ON ( 1 = 1 )
        WHERE
            a.stat_plan = 'E'
    ) load_sch
GROUP BY
    scen_name,
    scen_version,
    time,
    frequency
ORDER BY
    version)
select  distinct cte_lp.LP_SCEN
--,sls.IND_ENABLED
--,sls.LP_STEP_NAME
from cte_lp
    inner join SNP_LOAD_PLAN slp
        on cte_lp.LP_SCEN = slp.load_plan_name
    inner join snp_lp_step sls
        on slp.i_load_plan = sls.i_load_plan
        and LP_STEP_NAME = 'root_step'
where 1=1
and sls.IND_ENABLED =1
order by 1) tabl
;

--get all active load plans

/*Manually remove 4 extra UBE LPs from the result- 6am,daily,sun,sat*/
select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1
        order by 1
;

--get scenario names

select distinct lp.load_plan_name
, step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
order by lp.load_plan_name
;

---additional child scenarios----

select distinct sess_name
from snp_session
where sess_name not like '%BatchID%'
and sess_name not like '%FTP_INX_EMPLOYEES_DIBI_%'
minus
select distinct step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
;--add 2 to the total count to include the scn excluded by the filters


---get mapping names----

with master_list
as
(
select distinct lp.load_plan_name
, step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
order by lp.load_plan_name)
select master_list.*
, step.step_name
, step.table_name target_table
---, step.LSCHEMA_NAME
, scen.scen_no
from master_list
    left outer join SNP_SCEN scen
        on master_list.scen_name = scen.scen_name
    left outer join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
        and step.step_type = 'M'
order by master_list.load_plan_name,master_list.scen_name
;



---get all components ----
with master_list
as
(
select distinct lp.load_plan_name
, step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
order by lp.load_plan_name)
select master_list.*
, step.step_name
, case
	when step.step_type = 'CD'	then 'Check Datastore'
	when step.step_type = 'CM'	then 'Check Model'
	when step.step_type = 'CS'	then 'Check Sub-Model'
	when step.step_type = 'F'	then 'Interface'
	when step.step_type = 'JD'	then 'Journalize Datastore'
	when step.step_type = 'JM'	then 'Journalize Model'
	when step.step_type = 'JS'	then 'Journalize Sub-Model'
	when step.step_type = 'OE'	then 'Operating System Command'
	when step.step_type = 'RD'	then 'Datastore Reverse-engineering'
	when step.step_type = 'RM'	then 'Reverse Model'
	when step.step_type = 'RS'	then 'Sub-Model Reverse-engineering'
	when step.step_type = 'SE'	then 'Oracle Data Integrator Command'
	when step.step_type = 'T'	then 'Procedure'
	when step.step_type = 'V'	then 'Refresh Variable'
	when step.step_type = 'VD'	then 'Declare Variable'
	when step.step_type = 'VE'	then 'Evaluate Variable'
	when step.step_type = 'VP'	then 'Populate Variable'
	when step.step_type = 'VS'	then 'Set Variable'
	when step.step_type = 'M'	then 'Mapping'
	else 'Unknown'
end step_type
, step.table_name target_table
from master_list
    left outer join SNP_SCEN scen
        on master_list.scen_name = scen.scen_name
    left outer join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
--        and step.step_type = 'M'
order by master_list.load_plan_name,master_list.scen_name,step.nno
;


---UBE count
with cte
as
(select distinct lp.load_plan_name
, step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
)
select count(1)
from cte
where cte.scen_name in ( 'PKG_JDEE','PKG_JDRAIL')
;





----New qry to get LPs

WITH DIS
AS
(
select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =0
UNION 
SELECT DISTINCT SLP.load_plan_name
FROM SNP_LOAD_PLAN slp
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                AND sls.STEP_ORDER = 0
        where 1=1
        and sls.IND_ENABLED =0
ORDER BY 1)
SELECT DIS.*
FROM DIS
WHERE DIS.LOAD_PLAN_NAME NOT IN 
(select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1
)
AND DIS.LOAD_PLAN_NAME NOT LIKE '%FULL'
;


--child mappings--

with child_scen
as
(select distinct sess_name
from snp_session
where sess_name not like '%BatchID%'
and sess_name not like '%exclude_child%'
minus
select distinct step.scen_name 
from snp_load_plan  lp
    inner join snp_lp_step  step
    on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
where lp.load_plan_name in 
        (select distinct slp.load_plan_name
        from SNP_LP_INST INST
            inner join SNP_LOAD_PLAN slp
                on INST.load_plan_name = slp.load_plan_name
            inner join snp_lp_step sls
                on slp.i_load_plan = sls.i_load_plan
                and sls.LP_STEP_NAME = 'root_step'
        where 1=1
        and sls.IND_ENABLED =1)
)
select child_scen.sess_name,step.step_name
from child_scen
    inner join SNP_SCEN scen
        on child_scen.sess_name = scen.scen_name
    left outer join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
        and step.step_type = 'M'
;

---snow/ecosys/tandem/pms/pes
select distinct scen.scen_name
, step.step_name
, step.table_name target_table
, scen.scen_no
from SNP_SCEN scen
    inner join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
        and step.step_type = 'M'
where scen.scen_name in 
('PKG_NAME')
order by 1
;


--LP Scen mappings--

select lp.load_plan_name
, scen.scen_name
, step.step_name
from snp_load_plan  lp
    inner join snp_lp_step  step
        on lp.i_load_plan = step.i_load_plan
        and step.IND_ENABLED = 1
        and step.LP_STEP_TYPE = 'RS'
        and lp.load_plan_name in ('LP_NAME')
    left outer join SNP_SCEN scen
        on step.scen_name = scen.scen_name
    left outer join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
        and step.step_type = 'M'

;

