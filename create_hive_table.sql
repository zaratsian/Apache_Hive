

beeline -u jdbc:hive2://dzaratsian2.field.hortonworks.com:10000/default -n hive


################################################
#   Create Table
################################################

CREATE TABLE IF NOT EXISTS ztable ( 
    uid int, 
    product String,
    price float,
    quantity int)
COMMENT 'DZ Test Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC;


show tables;


describe formatted ztable;


################################################
# Insert Events
################################################

INSERT INTO TABLE ztable 
    select 1001, 'surfboard', 600.00, 1
    union all
    select 1002, 'surfboard', 1000.00, 4
    union all
    select 1003, 'wakeboard', 400, 1
    union all
    select 1004, 'wakeboard', 500, 1
    union all
    select 1005, 'boxing gloves', 50, 1
    union all
    select 1006, 'soccer ball', 25, 1
    ;


select * from ztable;



#ZEND
