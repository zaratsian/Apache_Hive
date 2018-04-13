
# https://community.hortonworks.com/articles/972/hive-and-xml-pasring.html

hdfs dfs -mkdir -p /test/sequences 

vi sample.xml 
<sequences period="5">
             <moment game-clock="300.00" time="1433732215737" game-event-id="" shot-clock="24.00" locations="-1,-1,96.95182,1.98648,5.75987;9,173004,45.54661,17.35545,0;9,338365,24.04722,25.67399,0;9,457611,46.95292,27.93478,0;9,468895,48.59834,33.96586,0;9,552336,33.73381,24.05929,0;5,214152,59.26872,24.12006,0;5,253997,45.71551,17.41071,0;5,457186,48.59834,33.96586,0;5,531447,78.09629,34.24688,0;5,552806,47.79678,22.8155,0"/> 
             <moment game-clock="300.00" time="1433732215794" game-event-id="" shot-clock="24.00" locations="-1,-1,97.79683,.89407,3.67626;9,173004,45.62283,17.34854,0;9,338365,24.04248,25.6784,0;9,457611,46.84978,27.8463,0;9,468895,48.52017,33.89189,0;9,552336,33.74064,24.03493,0;5,214152,59.27282,24.07895,0;5,253997,45.87101,17.38532,0;5,457186,48.52017,33.89189,0;5,531447,78.06394,34.2155,0;5,552806,47.8269,22.81393,0"/> 
             <moment game-clock="300.00" time="1433732215829" game-event-id="" shot-clock="24.00" locations="-1,-1,97.78946,.91006,3.68332;9,173004,45.61995,17.35703,0;9,338365,24.03815,25.68402,0;9,457611,46.71077,27.71191,0;9,468895,48.37095,33.77756,0;9,552336,33.74769,24.00829,0;5,214152,59.27627,24.06055,0;5,253997,46.00077,17.36555,0;5,457186,48.37095,33.77756,0;5,531447,78.0439,34.20521,0;5,552806,47.84297,22.83292,0"/> 
             <moment game-clock="300.00" time="1433732215856" game-event-id="" shot-clock="24.00" locations="-1,-1,97.73786,1.02206,3.73271;9,173004,45.57851,17.34979,0;9,338365,24.04207,25.61049,0;9,457611,46.63871,27.56226,0;9,468895,48.2033,33.7142,0;9,552336,33.75497,23.97935,0;5,214152,59.27906,24.06485,0;5,253997,46.10481,17.35141,0;5,457186,48.29748,33.63262,0;5,531447,78.03618,34.216,0;5,552806,47.84498,22.87247,0"/> 
             <moment game-clock="300.00" time="1433732215905" game-event-id="" shot-clock="24.00" locations="-1,-1,97.59781,1.32606,3.8668;9,173004,45.57865,17.34643,0;9,338365,24.04224,25.61058,0;9,457611,46.56615,27.44014,0;9,468895,48.01722,33.7018,0;9,552336,33.76247,23.94813,0;5,214152,59.27976,24.07223,0;5,253997,46.26668,17.38672,0;5,457186,48.29974,33.45708,0;5,531447,78.02931,34.2208,0;5,552806,47.86752,22.85019,0"/>
</sequences>

hdfs dfs -put sample.xml /test/sequences/
wget http://search.maven.org/remotecontent?filepath=com/ibm/spss/hive/serde2/xml/hivexmlserde/1.0.5.3/hivexmlserde-1.0.5.3.jar 
mv remotecontent?filepath=com%2Fibm%2Fspss%2Fhive%2Fserde2%2Fxml%2Fhivexmlserde%2F1.0.5.3%2Fhivexmlserde-1.0.5.3.jar hivexmlserde-1.0.5.3.jar
mv hivexmlserde-1.0.5.3.jar /tmp 

hive

add jar /tmp/hivexmlserde-1.0.5.3.jar; 
drop table sequences;

CREATE EXTERNAL TABLE sequences(
       gameclock double,
       time bigint,
       gameeventid string,
       shotclock double,
       locations string
     )
     ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
     WITH SERDEPROPERTIES (
     "column.xpath.gameclock"="/moment/@game-clock",
     "column.xpath.time"="/moment/@time",
     "column.xpath.gameeventid"="/moment/@game-event-id",
     "column.xpath.shotclock"="/moment/@shot-clock",
     "column.xpath.locations"="/moment/@locations"
     )
     STORED AS
     INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
     OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
     LOCATION '/test/sequences'
     TBLPROPERTIES (
     "xmlinput.start"="<moment ",
     "xmlinput.end"="/>"
     )
     ;

select * from sequences;

#ZEND
