

--###############################################################################################################
--#
--#  Performance Testing 
--#
--###############################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS testdata
    (id string, ssn string, amount int, password string, cc string, dob string, datestr string, cei string, name string, email string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS TEXTFILE
    LOCATION "/demo/ncaa/testdata"
    TBLPROPERTIES ("skip    .header.line.count"="1");

CREATE TABLE IF NOT EXISTS testdata_orc
    (id string, ssn string, amount int, password string, cc string, dob string, datestr string, cei string, name string, email string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE testdata_orc SELECT * FROM testdata;

SELECT COUNT(*) FROM testdata;
SELECT COUNT(*) FROM testdata_orc;

ANALYZE TABLE testdata COMPUTE STATISTICS;
ANALYZE TABLE testdata COMPUTE STATISTICS FOR COLUMNS;

SELECT COUNT(*) FROM testdata;




#ZEND