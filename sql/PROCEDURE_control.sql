
-- DELIMITER $$
-- CREATE PROCEDURE CHECK_FILE_CURRENT_IN_FTP (
-- IN ID_PARAM INT
-- )
-- BEGIN
-- 	SELECT timeLoad FROM LOG WHERE sourceId = ID_PARAM LIMIT 1;
-- END
-- $$
-- DROP PROCEDURE CHECK_FILE_CURRENT_IN_FTP
-- 
-- DROP PROCEDURE GET_TIMELOAD
-- DELIMITER $$
-- CREATE PROCEDURE GET_TIMELOAD(
-- IN ID_PARAM INT
-- )
-- BEGIN
-- SELECT timeLoad from log WHERE sourceId =ID_PARAM ORDER BY timeLoad ASC limit 1;
-- END$$
-- SELECT timeLoad from log WHERE 1 ORDER BY timeLoad ASC limit 1;
-- 

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
	SELECT `status`, id FROM log WHERE sourceId = sourceId_PARAM AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE limit 1;
END
$$


DROP PROCEDURE get_one_row_from_log

BEGIN
	SELECT * FROM log WHERE sourceId = sourceId_PARAM AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE limit 1;
END$$
SELECT count(*) FROM log WHERE sourceId = 1 AND timeLoad = HOUR(CURRENT_TIME) AND dateLoad = CURRENT_DATE limit 1;
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
IN ID_PARAM INT
)
BEGIN
	UPDATE log SET timeLoad = HOUR(CURRENT_TIME) AND dateLoad=CURRENT_DATE  WHERE ID = ID_PARAM;
END$$

-- DROP PROCEDURE update_time_load

DELIMITER $$
CREATE PROCEDURE INSERT_RECORD(
IN ID_PARAM INT,
IN CONFIG_ID_PARAM INT
)
BEGIN
	INSERT INTO LOG VALUES (ID_PARAM, CONFIG_ID_PARAM, CURRENT_DATE,HOUR(CURRENT_TIME), 'ER');
END$$

DELIMITER $$
CREATE PROCEDURE FINISH_LOAD_WEATHER_DATA_INTO_STAGING(
IN ID_PARAM INT,
IN TIME_LOAD_PARAM datetime
)
BEGIN
	UPDATE LOG SET `status` ='EL'WHERE sourceId = ID_PARAM AND timeLoad =TIME_LOAD_PARAM AND `status` = 'EO';
END
$$