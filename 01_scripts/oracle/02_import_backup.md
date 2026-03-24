1. Wait until Oracle is ready to use in docker
2. Copiati dump file-ul in docker data: docker cp fdbo_backup.dmp oracle-xe-21c:/opt/oracle/admin/XEPDB1/dpdump/fdbo_backup.dmp
3. Run la import: docker exec oracle-xe-21c impdp system/Oracle21c@//localhost:1521/XEPDB1 schemas=FDBO directory=DATA_PUMP_DIR dumpfile=fdbo_backup.dmp logfile=fdbo_import.log
