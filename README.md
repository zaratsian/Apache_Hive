<img src="images/Apache_Hive_logo.png" class="inline"/>&ensp;&ensp;<b>Hive Syntax & Projects</b>
<br>
<br><b>Commands:</b>
<br>
<br>show databases;
<br>show tables;
<br>
<br>describe formatted test_table;
<br>describe formatted test_table partition (my_column='my_value');
<br>
<br>CREATE TABLE test_table (name VARCHAR(64), age INT, rating DECIMAL(3, 2));
<br>
<br>INSERT INTO TABLE test_table VALUES ('frank', 33, 9.28), ('dean', 35, 8.32);
<br>
<br>
<br><b>Beeline:</b>
<br>```beeline -u jdbc:hive2://localhost:10000/default -n username -w password_file```
<br>```!connect jdbc:hive2://localhost:10015```
<br><a href="https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients">HiveServer2 Clients - Beeline</a>
<br>
<br>
<br><a href="create_hive_table.sql">Simple Example - Create Hive Table and Insert Records</a>
<br>
<br>
<br><b>References:</b>
<br><a href="http://hortonworks.com/wp-content/uploads/2016/05/Hortonworks.CheatSheet.SQLtoHive.pdf">Hortonworks hive Cheatsheet</a>
