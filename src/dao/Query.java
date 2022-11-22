package dao;

public interface Query {
	//control
	String GET_DB_HOSTING = "select host, username, password from dbconfig where type = ? and `using` = ?";
	String GET_FTP_HOSTING = "select host, port, username, password from ftpconfig where `using` = ?";

	String GET_URL_SOURCE = "select url from sourceconfig where id = ?";
	String GET_FILENAME_SOURCE = "select fileName from sourceconfig where id = ?";

	String GET_PATH_FOLDER_SOURCE = "select path_folder from sourceconfig where id = ?";
	String GET_DIST_FOLDER_SOURCE = "select dist_folder from sourceconfig where id = ?";

	String INSERT_LOG_DEFAULT = "insert into log values (?,?,?,?,?)";
	String SET_STATUS_WITH_ID_IN_LOG = "update log set status = ? where id = ?";
	String SET_STATUS_WITH_FTPPATH_IN_LOG = "update log set status = ? where path_ftp = ?";

	String CKECK_EXTRACTED_HOUR_CURRENT= "SELECT id from log where YEAR(time_load) = YEAR(NOW()) AND MONTH(time_load) = MONTH(NOW()) AND DAY(time_load) = DAY(NOW()) AND HOUR(time_load) = HOUR(NOW()) AND source_id = ? AND status = 'EO'";

	String GET_PATH_SOURCE_WITH_STATE_IN_LOG = "select path_ftp from log where status = ? and source_id = ?";
	String GET_LOG_WITH_PROVINCE_STATUS_EL = "select id from log where source_id = 3 and status = 'EL'";
	String GET_LOG_WITH_WEATHER_STATUS_EL = "select id from log where source_id in (1,2) and status = 'EL'";
	
	//stagging
	String INSERT_DATA_TO_DATE_DIM_STAGGING = "insert into datedim values (?,?,?,?,?,?,?,?)";
	String INSERT_DATA_TO_PROVICE = "insert into provincedim values (?,?)";
	String INSERT_DATA_TO_WEATHER_FACT_STAGGING = "insert into weatherfact values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	String GET_ID_BY_PROVINCE_NAME_STAGGING = "select id from provincedim where name_province = ?";
	String GET_ID_FROM_WEATHER_FACT_BY_PROVINCE = "select id from weatherfact where province_id = ?";
	String GET_DATA_FROM_DATE_DIM_STAGGING = "select id, date, month, year, hour, minute, second, day_of_week from datedim";
	String GET_DATA_FROM_PROVINCE_DIM_STAGGING = "select id, name_province from provincedim";
	String GET_DATA_FROM_WEATHERFACT_STAGGING = "select id, province_id, date_id, current_temperature, lowest_temperature, highest_temperature, humidity, overview, wind, vision, stop_point, uv_index, air_quality from weatherfact";
	String GET_PROVINCE_NAME_BY_ID_STAGGING = "select name_province from provincedim where id = ?";
	
	String DELETE_ALL_DATEDIM_STAGGING = "delete from datedim";
	String DELETE_ALL_PROVINCEDIM_STAGGING = "delete from provincedim";
	String DELETE_ALL_WEATHERFACT_STAGGING = "delete from weatherfact";
	
	//datawarehouse
	String INSERT_DATA_TO_DATE_DIM_DATAWAREHOUSE = "insert into datedim values (?,?,?,?,?,?,?,?)";
	String INSERT_DATA_TO_PROVICE_DATAWAREHOUSE = "insert into provincedim values (?,?)";
	String GET_ID_BY_PROVINCE_NAME_DATAWAREHOUSE = "select id from provincedim where name_province = ?";
	String INSERT_DATA_TO_WEATHER_FACT_DATAWAREHOUSE = "insert into weatherfact (sk,natural_key, province_id, date_id, current_temperature, lowest_temperature, highest_temperature, humidity, overview, wind, vision, stop_point, uv_index, air_quality, deleted, updated) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	String SET_TIMEEXPRIED_IN_WEATHER_FACT = "update weatherfact set time_expried = NOW() where YEAR(time_expried) = 9999";
}
