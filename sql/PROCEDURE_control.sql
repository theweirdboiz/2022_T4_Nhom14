-- SCRIPT 1
-- get url by id from sourceconfig
DELIMITER $$
CREATE PROCEDURE GET_URL_SOURCE (
IN source_id int
)
BEGIN
	SELECT url FROM sourceconfig WHERE id=source_id;
END
$$
-- get pathFolder by id from sourceconfig
DELIMITER $$
CREATE PROCEDURE GET_PATH_FOLDER (
IN source_id int
)
BEGIN
	SELECT pathFolder FROM sourceconfig WHERE id=source_id;
END
$$
select 

-- get distFolder by id from sourceconfig
DELIMITER $$
CREATE PROCEDURE GET_DIST_FOLDER (
IN source_id int
)
BEGIN
	SELECT distFolder FROM sourceconfig WHERE id=source_id;
END
$$

DELIMITER $$
CREATE PROCEDURE GET_DB_HOSTING (
IN dbName_param VARCHAR(20)
)
BEGIN
	SELECT * FROM dbconfig WHERE dbName = dbName_param;
END
$$

-- DROP PROCEDURE GET_FTP_HOSTING
DELIMITER $$
CREATE PROCEDURE GET_FTP_HOSTING (
IN ID_PARAM INT
)
BEGIN
	SELECT ftpconfig.* 
	from ftpconfig 
	join sourceconfig 
	on sourceconfig.ftp_id = ftpconfig.id
	WHERE sourceconfig.id = id_param;
END
$$

-- SELECT ftpconfig.* 
-- 	from ftpconfig 
-- 	join sourceconfig 
-- 	on sourceconfig.ftp_id = ftpconfig.id
-- 	WHERE sourceconfig.id = 3;

DROP PROCEDURE GET_FTP_HOSTING



DELIMITER $$
CREATE PROCEDURE GET_ONE_FILE_IN_FTP (
IN ID_PARAM INT
)
BEGIN
	SELECT * FROM LOG WHERE sourceId = ID_PARAM AND `status` = 'EO' ORDER BY dateLoad, timeLoad ASC LIMIT 1;
END
$$
drop PROCEDURE GET_ONE_FILE_IN_FTP

DELIMITER $$
CREATE PROCEDURE INSERT_RECORD(
IN id_param INT,
IN sourceId_param INT
)
BEGIN
	INSERT INTO log VALUES(id_param, sourceId_param, CURRENT_TIMESTAMP,'ER');
END$$

DELIMITER $$
CREATE PROCEDURE GET_ONE_ROW_FROM_LOG(
IN sourceId_PARAM INT
)
BEGIN
	SELECT id,`status` FROM log WHERE sourceId = sourceId_PARAM AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE limit 1;
END
$$

SELECT ID,`status` FROM LOG WHERE sourceId = 1 AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE LIMIT 1

-- DROP PROCEDURE get_one_row_from_log

SELECT id FROM log WHERE sourceId = 1 AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE limit 1;
-- drop PROCEDURE get_one_row_from_log
DELIMITER $$
CREATE PROCEDURE UPDATE_STATUS(
IN STATUS_PARAM VARCHAR(5),
IN ID_PARAM INT
)
BEGIN
	UPDATE log SET STATUS =STATUS_PARAM WHERE ID = ID_PARAM;
END$$

DELIMITER $$
CREATE PROCEDURE UPDATE_TIME_LOAD(
IN ID_PARAM INT,
IN CURRENT_HOUR_PARAM INT
)
BEGIN
	UPDATE log SET timeLoad = CURRENT_HOUR_PARAM, dateLoad=CURRENT_DATE  WHERE ID = ID_PARAM;
END$$

-- update log set timeLoad =16, dateLoad='2022-11-12' WHERE id = 92453246

-- DROP PROCEDURE update_time_load

DELIMITER $$
CREATE PROCEDURE INSERT_RECORD(
IN ID_PARAM INT,
IN CONFIG_ID_PARAM INT
)
BEGIN
	INSERT INTO LOG VALUES (ID_PARAM, CONFIG_ID_PARAM, CURRENT_DATE,HOUR(CURRENT_TIME), 'ER');
END$$

-- DELIMITER $$
-- CREATE PROCEDURE FINISH_LOAD_WEATHER_DATA_INTO_STAGING(
-- IN ID_PARAM INT,
-- IN TIME_LOAD_PARAM datetime
-- )
-- BEGIN
-- 	UPDATE LOG SET `status` ='EL'WHERE sourceId = ID_PARAM AND timeLoad =TIME_LOAD_PARAM AND `status` = 'EO';
-- END
-- $$
