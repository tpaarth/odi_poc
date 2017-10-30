
/*

SELECT add_months(prime.start_date,(level    -1))   start_date ,
  last_day(add_months(prime.start_date,(level-1)))  end_date ,
  decode(mod(extract( month from (add_months(prime.start_date,(level-1)))),3),1,'BQWAC',2,'MQWAC',0,'EQWAC') pricing_code_qualifier
FROM
  (SELECT trunc(to_Date('21-01-2015','dd-mm-yyyy'),'MM') start_date ,
    to_Date('01-12-2015','dd-mm-yyyy') end_date
  FROM dual
  ) prime
WHERE 1                                              =1
  CONNECT BY add_months(prime.start_date,(level-1)) <= prime.end_date
;

*/




declare
  cursor c_data
  is
  select *
  from GTN_IPI_3_STG
  where 1=1
  and item_uom in ('Un','EACH')
  and pricing_code_qualifier='WAC'
  ;
  
  cursor c_wac
  is
  WITH tab AS
  (SELECT ADD_MONTHS( PARAM.start_date, (LEVEL-1) )               AS mstart ,
    ADD_MONTHS( PARAM.start_date, (LEVEL) )-1                     AS mend ,
    TO_CHAR(ADD_MONTHS(pARAM.start_date, (LEVEL-1) ),'Q')         as qtr ,
    extract (YEAR FROM ADD_MONTHS( PARAM.start_date, (LEVEL -1) )) as  yr ,
    extract (MONTH FROM ADD_MONTHS( PARAM.start_date, (LEVEL-1) )) as mth
  FROM
    (SELECT TO_DATE('01/01/2015','mm/dd/yyyy') AS start_date ,
      TO_DATE(add_months(sysdate,7))           AS end_date
    FROM DUAL
    ) PARAM
    CONNECT BY ADD_MONTHS( TRUNC(PARAM.start_date, 'Q'), (LEVEL) ) -1 < PARAM.end_date
  )
  SELECT tab.mstart AS start_date ,
    tab.mend        AS end_date ,
    DECODE(row_number() over (partition BY tab.qtr,tab.yr order by tab.mth),1,'BQWAC'
                                                                           ,2,'MQWAC'
                                                                           ,3,'EQWAC') pricing_code_qualifier
  FROM tab 
  order by start_date
  ;
  
  v_pricing_code_qual varchar2(10);
  v_nom number :=0 ;  
  v_insrt number :=0 ;  
  v_ainsrt number :=0 ; 
  v_avgqwac_price number:=0;
begin

  for item in c_data
  loop <<main_loop>>
    
    for rec in c_wac
    loop <<wac_loop>>
      item.pricing_code_qualifier:=rec.pricing_code_qualifier;
      item.start_date:=to_char(rec.start_date,'yyyy-mm-dd');
      item.end_date:=to_char(rec.end_date,'yyyy-mm-dd');
      insert into GTN_IPI_3_STG values item;
      v_insrt:=v_insrt+1;
      ---dbms_output.put_line('here::');
    end loop wac_loop;
            
  end loop main_loop;
  
  for item in c_data
  loop <<avg_loop>>
    select round(avg(item_price),2)
    into v_avgqwac_price
    from GTN_IPI_3_STG
    where 1=1
    and item_uom in ('Un')
    and pricing_code_qualifier in ('BQWAC','EQWAC','MQWAC')
    and item_no=item.item_no
    ;
      
    item.item_price:= v_avgqwac_price;
    item.pricing_code_qualifier:='AVGQWAC';
    insert into GTN_IPI_3_STG values item;
      v_ainsrt:=v_ainsrt+1;
  end loop avg_loop;

  commit;

exception
  when others
  then 
    dbms_output.put_line('ERROR::'||sqlerrm||' line::'||dbms_utility.format_error_backtrace);
end;
