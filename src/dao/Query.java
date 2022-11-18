package dao;

public interface Query {
	//control
	String GET_DB_HOSTING = "select host, username, password from dbconfig where type = ? and `using` = ?";
	String GET_FTP_HOSTING = "select host, port, username, password from ftpconfig where `using` = ?";

	String GET_URL_SOURCE = "select url from sourceconfig where id = ?";
	String GET_FILENAME_SOURCE = "select fileName from sourceconfig where id = ?";

	String GET_PATH_FOLDER_SOURCE = "select pathFolder from sourceconfig where id = ?";
	String GET_DIST_FOLDER_SOURCE = "select distFolder from sourceconfig where id = ?";

	String INSERT_LOG_DEFAULT = "insert into log values (?,?,?,?,?)";
	String SET_STATUS_WITH_ID_IN_LOG = "update log set status = ? where id = ?";
	String SET_STATUS_WITH_FTPPATH_IN_LOG = "update log set status = ? where pathFTP = ?";

	String CKECK_EXTRACTED_HOUR_CURRENT= "SELECT id from log where YEAR(timeLoad) = YEAR(NOW()) AND MONTH(timeLoad) = MONTH(NOW()) AND DAY(timeLoad) = DAY(NOW()) AND HOUR(timeLoad) = HOUR(NOW()) AND sourceId = ? AND status = 'EO'";

	String GET_PATH_SOURCE_WITH_STATE_IN_LOG = "select pathFTP from log where status = ? and sourceId = ?";
	String GET_LOG_WITH_PROVINCE_STATUS_EL = "select id from log where sourceId = 3 and status = 'EL'";
	String GET_LOG_WITH_WEATHER_STATUS_EL = "select id from log where sourceId in (1,2) and status = 'EL'";
	
	//stagging
	String INSERT_DATA_TO_DATE_DIM_STAGGING = "insert into datedim values (?,?,?,?,?,?,?,?)";
	String INSERT_DATA_TO_PROVICE = "insert into provincedim values (?,?)";
	String INSERT_DATA_TO_WEATHER_FACT_STAGGING = "insert into weatherfact values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	String GET_ID_BY_PROVINCE_NAME_STAGGING = "select id from provincedim where nameProvince = ?";
	String GET_ID_FROM_WEATHER_FACT_BY_PROVINCE = "select id from weatherfact where provinceId = ?";
	String GET_DATA_FROM_DATE_DIM_STAGGING = "select id, date, month, year, hour, minute, second, dayOfWeek from datedim";
	String GET_DATA_FROM_PROVINCE_DIM_STAGGING = "select id, nameProvince from provincedim";
	String GET_DATA_FROM_WEATHERFACT_STAGGING = "select id, provinceId, dateId, currentTemperature, lowestTemperature, highestTemperature, humidity, overview, wind, vision, stopPoint, uvIndex, airQuality from weatherfact";
	String GET_PROVINCE_NAME_BY_ID_STAGGING = "select nameProvince from provincedim where id = ?";
	
	String DELETE_ALL_DATEDIM_STAGGING = "delete from datedim";
	String DELETE_ALL_PROVINCEDIM_STAGGING = "delete from provincedim";
	String DELETE_ALL_WEATHERFACT_STAGGING = "delete from weatherfact";
	
	//datawarehouse
	String INSERT_DATA_TO_DATE_DIM_DATAWAREHOUSE = "insert into datedim values (?,?,?,?,?,?,?,?)";
	String INSERT_DATA_TO_PROVICE_DATAWAREHOUSE = "insert into provincedim values (?,?)";
	String GET_ID_BY_PROVINCE_NAME_DATAWAREHOUSE = "select id from provincedim where nameProvince = ?";
	String INSERT_DATA_TO_WEATHER_FACT_DATAWAREHOUSE = "insert into weatherfact (sk,naturalKey, provinceId, dateId, currentTemperature, lowestTemperature, highestTemperature, humidity, overview, wind, vision, stopPoint, uvIndex, airQuality, deleted, updated) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	String SET_TIMEEXPRIED_IN_WEATHER_FACT = "update weatherfact set timeExpried = NOW() where YEAR(timeExpried) = 9999";
}
