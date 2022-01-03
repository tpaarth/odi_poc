# PROCEDURE to list filenames and insert into a table

import java.lang as lang
import java.sql as sql
import java.lang.String
import os
import java.io.File as File
import time


#db connection details
driverSrc = 'oracle.jdbc.driver.OracleDriver'
lang.Class.forName(driverSrc)
urlSrc = '#GLOBAL.GV_DB_URL'
userSrc = '#GLOBAL.GV_DB_USER'
passwdSrc = '#GLOBAL.GV_DB_PWD'


#read directory and insert list of files to batch_session_log or rel_batch_session_log
ConSrc = sql.DriverManager.getConnection(urlSrc, userSrc, passwdSrc);
readDBLink = ConSrc.createStatement()
odi_sess='<%=odiRef.getSession("SESS_NAME")%>'+':<%=odiRef.getSession("SESS_NO")%>'

sqlTAB = "insert into vai_gold.batch_session_log (batch_id,batch_name, source_info, status, start_time) values ("
dirName = '#PROJ_VAI.PV_SOURCE_DIR'+'/src'

for file in os.listdir(dirName):
    millis = int(round(time.time() * 1000))
    sqlPARAMS="'"+str(millis)+"', '" +str(odi_sess)+"','"+ file + "', 'PL', SYSDATE)"
    sqlDBLink = sqlTAB + sqlPARAMS
    rqteDBLink = readDBLink.execute(sqlDBLink)

ConSrc.close()
