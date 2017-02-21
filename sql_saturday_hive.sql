
SHOW TABLES;

###############################################################################################################
#
#   Hive - Load Data
#
###############################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS mm_season
    (season string, day string, wteam string, wscore int, lteam string, lscore int, wloc string, ot string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS TEXTFILE
    LOCATION "/tmp/marchmadness/SeasonResults"
    TBLPROPERTIES ("skip.header.line.count=1");

CREATE TABLE IF NOT EXISTS mm_season_orc
    (season string, day string, wteam string, wscore int, lteam string, lscore int, wloc string, ot string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE mm_season_orc SELECT * FROM mm_season;

SELECT * FROM mm_season_orc LIMIT 10;

SELECT COUNT(*) FROM mm_season_orc;

DESCRIBE mm_season_orc;

###############################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS mm_teams
    (team_id string, team_name string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS TEXTFILE
    LOCATION "/tmp/marchmadness/Teams"
    TBLPROPERTIES ("skip.header.line.count=1");

CREATE TABLE IF NOT EXISTS mm_teams_orc
    (team_id string, team_name string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE mm_teams_orc SELECT * FROM mm_teams;

SELECT * FROM mm_teams_orc LIMIT 10;

SELECT COUNT(*) FROM mm_teams_orc;

DESCRIBE mm_teams_orc;

###############################################################################################################
#
#   Hive - Join (as materialized table)
#
###############################################################################################################

CREATE TABLE IF NOT EXISTS mm_join1 AS
    SELECT mm_season_orc.*, 
        teams2a.team_name AS WTEAM_NAME, 
        teams2b.team_name AS LTEAM_NAME
    FROM mm_season_orc
    LEFT JOIN mm_teams_orc teams2a ON (mm_season_orc.wteam = teams2a.team_id)
    LEFT JOIN mm_teams_orc teams2b ON (mm_season_orc.lteam = teams2b.team_id);

SELECT * FROM mm_join1 LIMIT 10;
    
DESC mm_join1;

SELECT COUNT(*) FROM mm_season_orc;
SELECT COUNT(*) FROM mm_teams_orc;
SELECT COUNT(*) FROM mm_join1;

--SHOW CREATE TABLE mm_join1;

###############################################################################################################
#
#   Hive - Join (as view)
#
###############################################################################################################

CREATE VIEW IF NOT EXISTS mm_join1_view AS
    SELECT mm_season_orc.*, 
        teams2a.team_name AS WTEAM_NAME, 
        teams2b.team_name AS LTEAM_NAME
    FROM mm_season_orc
    LEFT JOIN mm_teams_orc teams2a ON (mm_season_orc.wteam = teams2a.team_id)
    LEFT JOIN mm_teams_orc teams2b ON (mm_season_orc.lteam = teams2b.team_id);


###############################################################################################################
#
#   Hive - Aggregation
#
###############################################################################################################

SELECT WTEAM_NAME, COUNT(*) AS WINS 
    FROM mm_join1 
    GROUP BY WTEAM_NAME 
    ORDER BY WINS DESC 
    LIMIT 15;

SELECT LTEAM_NAME, COUNT(*) AS LOSSES 
    FROM mm_join1 
    GROUP BY LTEAM_NAME 
    ORDER BY LOSSES DESC 
    LIMIT 15;

SELECT SEASON, WSCORE, LSCORE, WLOC, (WSCORE-LSCORE) AS SCORE_DIFF, CONCAT(WTEAM_NAME, " OVER ", LTEAM_NAME) as DESC
    FROM mm_join1
    ORDER BY SCORE_DIFF DESC
    LIMIT 10;


#ZEND
