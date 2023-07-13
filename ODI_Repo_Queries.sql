/*LOAD PLAN related queries*/
---see load_plan folder

select load_plan_name,f.SCEN_FOLDER_NAME,pf.SCEN_FOLDER_NAME parent_folder
from snp_load_plan  lp
    inner join snp_scen_folder f
        on lp.I_SCEN_FOLDER = f.I_SCEN_FOLDER
    left outer join snp_scen_folder pf
        on f.par_i_scen_folder = pf.I_SCEN_FOLDER
where 1=1
and lp.load_plan_name like '%KOFAX%'


---see load_plan steps
select i_lp_step,lp_step_name
,lp_step_type
,scen_name
,var_name
,var_op
,var_value
,EXCEPT_BEHAVIOR
from SNP_LP_STEP
where 1=1
and i_load_plan = 349
;

---see all load_plan runs
select *---I_LP_INST,i_load_plan
from SNP_LP_INST
where 1=1
and load_plan_name = 'LP_DIBI_KRONOSGS_SUBSCRIBER_INCR'
order by I_LP_INST desc
;

	--see all load_plan run status
	select *
	from SNP_LPI_RUN
	where I_LP_INST in (
	select I_LP_INST
	from SNP_LP_INST
	where 1=1
	and load_plan_name = 'LP_PAYROLL_COSTING_FROM_ORAHRPGS_TO_DIBI'
	)
	order by I_LP_INST desc
	;
	

---see load_plan run session details
select step.lp_step_name
, step_log.status
,step_log.sess_no
,step.scen_name
,step_log.NB_ROW
, step_log.NB_INS
, step_log.NB_UPD
, step_log.NB_DEL
, step_log.NB_ERR
from SNP_LPI_STEP_LOG step_log
    inner join SNP_LP_STEP step
        on step_log.I_LP_STEP = step.I_LP_STEP
where 1=1
and I_LP_INST=10513163---10434881
order by step_log.I_LP_STEP
;


--see session details--
select  task.task_name1
, task.task_name2
, task.task_name3
, task_log.sess_no
, task_log.nno
, task_log.NB_ROW
, task_log.NB_INS
, task_log.NB_UPD
, task_log.NB_DEL
, task_log.NB_ERR
, task_log.DEF_TXT
, task_log.ERROR_MESSAGE
from snp_sess_task_log task_log
    left outer join snp_sess_task task
    on task.sess_no = task_log.sess_no
--    and task.nno = task_log.nno
    and task.scen_task_no = task_log.scen_task_no
where 1=1
and task_log.sess_no = 63574602
ORDER BY task_log.NNO,task_log.SCEN_TASK_NO
;


--search load_plan from scenario--
select *
from snp_lp_inst
where I_LP_INST in (
select I_LP_INST
from snp_lpi_step_log
where sess_no = 51563641)--scen sess number
;
	select i.LOAD_PLAN_NAME,r.*
	from snp_lp_inst i
	        inner join snp_lpi_run r
	        on i.I_LP_INST = r.I_LP_INST
	where i.I_LP_INST in (
	                    select I_LP_INST
	                    from snp_lpi_step_log
	                    where sess_no = 68928191)--scen sess number
	;
	


select *
from snp_sess_task_log
where sess_no = 63574602
ORDER BY SCEN_TASK_NO,NNO
;

select *
from snp_session
where sess_no = 63574602
;
select *
from snp_sess_task
where TASK_NAME2 = 'MAP_CF01000_30_KRONOSGS_PERSON_SUBSCRIBER_MAIN'
;

select *
from SNP_LPI_STEP_LOG
where I_LP_INST=10513163
;

--see session details 2--
SELECT
    ss.sess_no,
    ss.scen_name,
    ss.scen_version,
    ss.sess_name,
    ss.parent_sess_no,
    ss.sess_beg,
    ss.sess_end,
    ss.sess_status,
    decode(ss.sess_status, 'D', 'Done', 'E', 'Error',
           'M', 'Warning', 'Q', 'Queued', 'R',
           'Running', 'W', 'Waiting', ss.sess_status)                   AS sess_status_desc,
    ssl.nno,
    sstl.nb_run,
    sst.task_type,
    decode(sst.task_type, 'C', 'Loading', 'J', 'Mapping',
           'S', 'Procedure', 'V', 'Variable', sst.task_type)             AS task_type_desc,
    sst.exe_channel,
    decode(sst.exe_channel, 'B', 'Oracle Data Integrator Scripting', 'C', 'Oracle Data Integrator Connector',
           'J', 'JDBC', 'O',
           'Operating System',
           'Q',
           'Queue',
           'S',
           'Oracle Data Integrator Command',
           'T',
           'Topic',
           'U',
           'XML Topic',
           sst.exe_channel)                                          AS exe_channel_desc,
    sstl.scen_task_no,
    sst.par_scen_task_no,
    sst.task_name1,
    sst.task_name2,
    sst.task_name3,
    sstl.task_dur,
    sstl.nb_row,
    sstl.nb_ins,
    sstl.nb_upd,
    sstl.nb_del,
    sstl.nb_err,
    sss.lschema_name
    || '.'
    || sss.res_name                                                  AS target_table,
    CASE
        WHEN sst.col_tech_int_name IS NOT NULL
             AND sst.col_lschema_name IS NOT NULL THEN
            sst.col_tech_int_name
            || '.'
            || sst.col_lschema_name
        ELSE
            NULL
    END                                                              AS target_schema,
    sstl.def_txt                                                     AS target_command,
    CASE
        WHEN sst.def_tech_int_name IS NOT NULL
             AND sst.def_lschema_name IS NOT NULL THEN
            sst.def_tech_int_name
            || '.'
            || sst.def_lschema_name
        ELSE
            NULL
    END                                                             AS source_schema,
    sstl.col_txt                                                     AS source_command
FROM
         snp_session ss
    INNER JOIN snp_step_log       ssl ON ss.sess_no = ssl.sess_no
    INNER JOIN snp_sess_task_log  sstl ON ss.sess_no = sstl.sess_no
    INNER JOIN snp_sb_task        sst ON sstl.sb_no = sst.sb_no
                                  AND sstl.scen_task_no = sst.scen_task_no
                                  AND ssl.nno = sstl.nno
                                  AND sstl.nno = sst.nno
                                  AND ssl.nb_run = sstl.nb_run
    LEFT JOIN snp_sb_step        sss ON sst.sb_no = sss.sb_no
                                 AND sst.nno = sss.nno
WHERE
    ss.sess_no = 63574602
ORDER BY
    sess_no,
    nno,
    scen_task_no;

/*load plan to scneario*/
with cte_lp
as
(SELECT
    scen_name     AS ""LP_SCEN"",
    scen_version  AS ""VERSION"",
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
select distinct cte_lp.LP_SCEN
, sls.scen_name
from cte_lp
    inner join SNP_LOAD_PLAN slp
        on cte_lp.LP_SCEN = slp.load_plan_name
    inner join snp_lp_step sls
        on slp.i_load_plan = sls.i_load_plan
        and sls.lp_step_type = 'RS'
--where cte_lp.LP_SCEN = 'LP_DIBI_KRONOSGS_SUBSCRIBER_INCR'

------scenario---------

select task.*
from SNP_SCEN scen
    inner join SNP_SCEN_STEP step
        on scen.scen_no = step.scen_no
    inner join SNP_SCEN_TASK task
        on task.scen_no = scen.scen_no
where 1=1
and scen.scen_name = 'PKG_CF01000_30_KRONOSGS_PERSON_SUBSCRIBER_INCR'
;
/*Package related*/
---All steps(mappings only) in a package
select pack.pack_name
,   mapp.name mapping_name
from snp_package pack
    inner join snp_step step
        on pack.i_package = step.i_package
        and step.step_type = 'M'
    inner join snp_mapping mapp
        on mapp.i_mapping = step.i_mapping
where pack.pack_name IN ('PKG_CF20500_15_KRONOSGS_LOCATION_SUBSCRIBER')
order by step.i_package,step.i_step
;

---use this query
select distinct pack.pack_name
,   mapp.name mapping_name
from snp_package pack
    inner join snp_step step
        on pack.i_package = step.i_package
        and step.step_type = 'M'
    inner join snp_mapping mapp
        on mapp.i_mapping = step.i_mapping
where pack.pack_name IN (
'DEL_DUP_MULTI_REC_RECEIPTS',
'PKG_CF07480_105_KOFAX_PURCHASE_ORDERS_SUBSCRIBER_INCR',
'PKG_ERR_HANDLER_DIBI',
'PKG_CF07320_105_KOFAX_RECEIPT_SUBSCRIBER_INCR',
'LP_ERROR_HANDLER',
'PKG_ERR_HANDLER_KOFAX')
--order by step.i_package,step.i_step
order by 1
;


/*MAPPING  components*/

--Mapping details 1 - only target/source/km
select *
from
(with mb
  as
  (SELECT MAPP.NAME mapping_name
    ,mapp.i_mapping
    ,COMP.TYPE_NAME comp_type
    ,comp.name
    ,comp.i_map_comp
    ,comp.i_map_ref
    FROM SNP_MAPPING MAPP 
      inner JOIN SNP_MAP_COMP COMP ON MAPP.I_MAPPING = COMP.I_OWNER_MAPPING 
      inner join snp_folder folder on mapp.i_folder = folder.i_folder
      inner join snp_project proj on folder.i_project = proj.i_project
    WHERE 1=1
    and mapp.name       in('MAP_CF01000_30_KRONOSGS_PERSON_SUBSCRIBER_MAIN')---'WC_QUADS_PHASE_F_CAPA' ---Mapping name
    --and proj.project_name in ('COMPASS')
  )
  select distinct t.mapping_name
  , t.comp_type
  , listagg(t.descr,';') within group (order by t.descr) over (partition by t.comp_type,t.mapping_name) descr
  from
  (select mb.mapping_name
    , case 
        when cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
        then 'TARGET DS'
        else 'SOURCE DS'
      end comp_type
    , mr.qualified_name descr
    from mb 
      inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
      inner join snp_map_ref mr on mb.i_map_ref = mr.i_map_ref
      --inner join snp_table t on mr.i_ref_id = t.i_table
      --inner join snp_model mdl on t.i_mod = mdl.i_mod
    where  1=1
    and mb.comp_type IN ('DATASTORE','FILE')  
    and cp.direction = 'O' 
  )t
  union all
  select mb.mapping_name
  , 'KM' comp_type 
  , listagg(mr.qualified_name,';') within group (order by mr.qualified_name)descr
  from mb
    inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
    inner join SNP_PHY_NODE phy on cp.i_owner_map_comp = phy.i_map_comp
    inner join snp_map_ref mr on phy.i_tgt_comp_km=mr.i_map_ref
  where 1=1
  and cp.direction = 'O'
  and cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
  group by mb.mapping_name, 'KM' 
  )
pivot
  ( min(DESCR)
    FOR comp_type IN ('SOURCE DS' as source_datastores
                     ,'TARGET DS' as target_datastores
                     ,'KM' as knowledge_modules)
  )
ORDER BY 1;


--Mapping details 2
select *
from
(with mb
  as
  (SELECT MAPP.NAME mapping_name
    ,mapp.i_mapping
    ,COMP.TYPE_NAME comp_type
    ,comp.name
    ,comp.i_map_comp
    ,comp.i_map_ref
    FROM SNP_MAPPING MAPP 
      inner JOIN SNP_MAP_COMP COMP ON MAPP.I_MAPPING = COMP.I_OWNER_MAPPING 
      inner join snp_folder folder on mapp.i_folder = folder.i_folder
      inner join snp_project proj on folder.i_project = proj.i_project
    WHERE 1=1
    --and mapp.name       in('WC_C2P_MEDICAID_CLAIM_F','INT_ODS2SFTYREP_RECEIPT_XREF_LOAD_SEG1_SEG3 ')---'WC_QUADS_PHASE_F_CAPA' ---Mapping name
    and proj.project_name in ('COMPASS')
  )
  select mb.mapping_name
  , mb.comp_type
  , listagg(to_char(EXP.TXT),';') within group (order by mb.name) descr
  from mb
    inner join snp_map_prop prop on mb.I_MAP_COMP = prop.I_OWNER_MAP_COMP
    inner join SNP_MAP_PROP_DEF prop_def on PROP.I_PROP_DEF = PROP_DEF.I_MAP_PROP_DEF
    inner join snp_map_expr exp on PROP.I_MAP_PROP = EXP.I_OWNER_MAP_PROP
  where 1=1
  and prop_def.prop_name in ('FILTER_CONDITION','JOIN_CONDITION') group by mb.mapping_name, mb.comp_type
  union all
  select distinct t.mapping_name
  , t.comp_type
  , listagg(t.descr,';') within group (order by t.descr) over (partition by t.comp_type,t.mapping_name) descr
  from
  (select mb.mapping_name
    , case 
        when cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
        then 'TARGET DS'
        else 'SOURCE DS'
      end comp_type
    , mr.qualified_name descr
    from mb 
      inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
      inner join snp_map_ref mr on mb.i_map_ref = mr.i_map_ref
      --inner join snp_table t on mr.i_ref_id = t.i_table
      --inner join snp_model mdl on t.i_mod = mdl.i_mod
    where  1=1
    and mb.comp_type IN ('DATASTORE','FILE')  
    and cp.direction = 'O' 
  )t
  union all
  select distinct sm.name mapping_name
  , 'Description' comp_name
  , to_char(descript) descr
  from mb
    inner join SNP_MAPPING sm on mb.i_mapping= sm.i_mapping 
  union all
  select mb.mapping_name
  , 'KM' comp_type 
  , listagg(mr.qualified_name,';') within group (order by mr.qualified_name)descr
  from mb
    inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
    inner join SNP_PHY_NODE phy on cp.i_owner_map_comp = phy.i_map_comp
    inner join snp_map_ref mr on phy.i_tgt_comp_km=mr.i_map_ref
  where 1=1
  and cp.direction = 'O'
  and cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
  group by mb.mapping_name, 'KM' 
  union all
  select  mb.mapping_name
  , 'update key' comp_type
  , listagg(attr.name,';') within group (order by sort_pos) descr
  from mb
    inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
    inner join snp_map_attr attr on cp.I_MAP_CP = attr.I_OWNER_MAP_CP
    inner join snp_map_prop prop on attr.i_map_attr = prop.i_owner_map_attr
    inner join SNP_MAP_PROP_DEF prop_def on PROP.I_PROP_DEF = PROP_DEF.I_MAP_PROP_DEF
  where 1=1
  and cp.i_map_cp not in (select i_start_map_cp from snp_map_conn)
  and prop_def.prop_name like 'KEY_INDICATOR' group by mb.mapping_name
  )
pivot
  ( min(DESCR)
    FOR comp_type IN ('Description' as Description
                     ,'JOIN' as Join_conditions
                     ,'FILTER' as filter_conditons
                     ,'SOURCE DS' as source_datastores
                     ,'TARGET DS' as target_datastores
                     ,'KM' as knowledge_modules
                     ,'update key' as update_key)
  )
ORDER BY 1;


--Mapping details 3 - fewer details
select *
from
(with mb
  as
  (SELECT MAPP.NAME mapping_name
    ,mapp.i_mapping
    ,COMP.TYPE_NAME comp_type
    ,comp.name
    ,comp.i_map_comp
    ,comp.i_map_ref
    FROM SNP_MAPPING MAPP 
      inner JOIN SNP_MAP_COMP COMP ON MAPP.I_MAPPING = COMP.I_OWNER_MAPPING 
      inner join snp_folder folder on mapp.i_folder = folder.i_folder
      inner join snp_project proj on folder.i_project = proj.i_project
    WHERE 1=1
    and mapp.name       in(
'MAP_CF00050_15_KRONOSGS_COST_CODE_STRUCTURE_SUBSCRIBER_STG_ACT_ENTRIES_AND_LLE')---'WC_QUADS_PHASE_F_CAPA' ---Mapping name
    --and proj.project_name in ('COMPASS')
  )
  select distinct t.mapping_name
  , t.comp_type
  , listagg(t.descr,';') within group (order by t.descr) over (partition by t.comp_type,t.mapping_name) descr
  from
  (select mb.mapping_name
    , case 
        when cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
        then 'TARGET DS'
        else 'SOURCE DS'
      end comp_type
    , mr.qualified_name descr
    from mb 
      inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
      inner join snp_map_ref mr on mb.i_map_ref = mr.i_map_ref
      --inner join snp_table t on mr.i_ref_id = t.i_table
      --inner join snp_model mdl on t.i_mod = mdl.i_mod
    where  1=1
    and mb.comp_type IN ('DATASTORE','FILE')  
    and cp.direction = 'O' 
  )t
  union all
  select mb.mapping_name
  , 'KM' comp_type 
  , listagg(mr.qualified_name,';') within group (order by mr.qualified_name)descr
  from mb
    inner join snp_map_cp cp on mb.i_map_comp = cp.i_owner_map_comp
    inner join SNP_PHY_NODE phy on cp.i_owner_map_comp = phy.i_map_comp
    inner join snp_map_ref mr on phy.i_tgt_comp_km=mr.i_map_ref
  where 1=1
  and cp.direction = 'O'
  and cp.i_map_cp not in (select i_start_map_cp from snp_map_conn) 
  group by mb.mapping_name, 'KM' 
  )
pivot
  ( min(DESCR)
    FOR comp_type IN ('SOURCE DS' as source_datastores
                     ,'TARGET DS' as target_datastores
                     ,'KM' as knowledge_modules)
  )
ORDER BY 1;


