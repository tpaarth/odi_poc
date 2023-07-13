select tec.techno_name
, cn.CON_NAME
, tx.full_txt
, cn.user_name
from SNP_CONNECT cn
    inner join SNP_MTXT tx
        on cn.I_TXT_JAVA_URL = tx.i_txt
    inner join snp_techno tec
        on cn.I_TECHNO = tec.I_TECHNO
where cn.con_name in ('PHYSICAL_DATA_SERVER_NAME_HERE')
