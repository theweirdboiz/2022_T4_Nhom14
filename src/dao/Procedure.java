package dao;

public interface Procedure {

	String GET_DB_HOSTING = "{CALL GET_DB_HOSTING(?)}";
	String GET_URL_SOURCE = "{CALL GET_URL_SOURCE(?)}";
	String GET_PATH_FOLDER = "{CALL GET_PATH_FOLDER(?)}";
	String GET_DIST_FOLDER = "{CALL GET_DIST_FOLDER(?)}";

	String GET_FTP_HOSTING = "{CALL GET_FTP_HOSTING(?)}";

	// process1
	String CHECK_IS_FILE_LOADED = "{CALL CHECK_IS_FILE_LOADED(?,?)}";
	String GET_ONE_FILE_IN_FTP = "{CALL GET_ONE_FILE_IN_FTP(?)}";
	String INSERT_RECORD = "{CALL INSERT_RECORD(?,?,?,?,?)}";
	String UPDATE_STATUS = "{CALL UPDATE_STATUS(?,?)}";
	String UPDATE_TIME_LOAD = "{CALL UPDATE_TIME_LOAD(?,?)}";
	String GET_INFORMATION_CONFIG = "{CALL GET_INFORMATION_CONFIG(?)}";
	String GET_INFORMATION_LIMIT_1 = "{CALL GET_INFORMATION_LIMIT_1(?)}";

	// process2
	String GET_ONE_DIM = "{CALL GET_ONE_DIM(?)}";
	String GET_ONE_FACT = "{CALL GET_ONE_FACT()}";
	String LOAD_DATE_DIM_FILE = "{CALL LOAD_DATE_DIM_FILE(?)}";
	String GET_RAW_FACT = "{CALL GET_RAW_FACT()}";

	String GET_LOG_ID = "{CALL GET_LOG_ID()}";
	String GET_SOURCE_ID_BY_LOG_ID = "{CALL GET_SOURCE_ID_BY_LOG_ID(?)}";
	String GET_TIME_LOAD_BY_LOG_ID = "{CALL GET_TIME_LOAD_BY_LOG_ID(?)}";
	String GET_STATUS_BY_LOG_ID = "{CALL GET_STATUS_BY_LOG_ID(?)}";

	String IS_PROVINCE_DIM_EXISTED = "{CALL IS_PROVINCE_DIM_EXISTED(?,?)}";

	String GET_INFORMATION_LITMIT_1 = "{CALL GET_INFORMATION_LIMIT_1()}";
	String GET_ONE_ROW_INFO = "{CALL GET_ONE_ROW_INFORMATION()}";
	String LOAD_RAW_FACT = "{CALL LOAD_RAW_FACT(?,?,?,?,?,?,?,?,?,?,?,?,?)}";
	String TRANSFORM_RAW_FACT = "{CALL TRANSFORM_RAW_FACT(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";

	String CHECK_DATE_DIM_IS_EXISTED = "{CALL CHECK_DATE_DIM_IS_EXISTED()}";
	String CHECK_TIME_DIM_IS_EXISTED = "{CALL CHECK_TIME_DIM_IS_EXISTED()}";
	String LOAD_DATE_DIM = "{CALL LOAD_DATE_DIM(?,?,?,?,?,?)}";
	String LOAD_TIME_DIM = "{CALL LOAD_TIME_DIM(?,?)}";
	String LOAD_PROVINCE_DIM = "{CALL LOAD_PROVINCE_DIM(?,?,?)}";
	String GET_ALL_PROVINCEDIM_IN_STAGING = "{CALL GET_ALL_PROVINCEDIM_IN_STAGING()}";

//	process 3
	String GET_ONE_PROVINCEDIM_WITH_EL_STATUS = "{CALL GET_ONE_PROVINCEDIM_WITH_EL_STATUS()}";
	String GET_ALL_CURRENT_WEATHER_DATA = "{CALL GET_ALL_CURRENT_WEATHER_DATA()}";
	String INSERT_ALL_FROM_STAGING = "{CALL INSERT_ALL_FROM_STAGING()}";
	String LOAD_PROVINCE_DIM_INTO_WAREHOUSE = "{CALL LOAD_PROVINCE_DIM_INTO_WAREHOUSE(?,?,?)}";

}
