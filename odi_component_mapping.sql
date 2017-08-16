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
