package dao;

public interface Procedure {

	String GET_DB_HOSTING = "{CALL GET_DB_HOSTING(?)}";
	String GET_URL_SOURCE = "{CALL GET_URL_SOURCE(?)}";
	String GET_PATH_FOLDER = "{CALL GET_PATH_FOLDER(?)}";
	String GET_DIST_FOLDER = "{CALL GET_DIST_FOLDER(?)}";

	String GET_FTP_HOSTING = "{CALL GET_FTP_HOSTING(?)}";

	// process1
	String GET_ONE_ROW_FROM_LOG = "{CALL GET_ONE_ROW_FROM_LOG(?)}";
	String INSERT_RECORD = "{CALL INSERT_RECORD(?,?)}";
	String UPDATE_STATUS = "{CALL UPDATE_STATUS(?,?)}";
	String UPDATE_TIME_LOAD = "{CALL UPDATE_TIME_LOAD(?,?)}";

//	String START_EXTRACT = "{CALL START_EXTRACT(?,?)}";
//	String FINISH_EXTRACT = "{CALL FINISH_EXTRACT(?)}";
//	String FAIL_EXTRACT = "{CALL FAIL_EXTRACT(?)}";
//	String GET_CURRENT_SOURCE_ID = "{CALL GET_CURRENT_SOURCE_ID()}";
//	String GET_TIMELOAD = "{CALL GET_TIMELOAD(?)}";

	String GET_ONE_FILE_IN_FTP = "{CALL GET_ONE_FILE_IN_FTP(?)}";

	// process2
	String LOAD_WEATHER_DATA = "{CALL LOAD_DATA_WEATHER_INTO_STAGING(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
	String FINISH_LOAD_WEATHER_DATA_INTO_STAGING = "{CALL FINISH_LOAD_WEATHER_DATA_INTO_STAGING(?,?)}";
	String DELETE_DATE_DIM = "{CALL DELETE_DATE_DIM()}";
	String LOAD_DATE_DIM = "{CALL LOAD_DATE_DIM(?,?,?,?,?,?)}";
	String LOAD_TIME_DIM = "{CALL LOAD_TIME_DIM(?,?)}";
	String LOAD_PROVINCE_DIM = "{CALL LOAD_PROVINCE_DIM(?,?)}";

}
