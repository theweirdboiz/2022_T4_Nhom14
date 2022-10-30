package dao;

public interface Query {
	String GET_DB_HOSTING = "select host, username, password from dbconfig where type = ? and `using` = ?";
	String GET_FTP_HOSTING = "select host, port, username, password from ftpconfig where `using` = ?";

	String GET_URL_SOURCE = "select url from sourceconfig where id = ?";
	String GET_FILENAME_SOURCE = "select fileName from sourceconfig where id = ?";

	String GET_PATH_FOLDER_SOURCE = "select pathFolder from sourceconfig where id = ?";
	String GET_DIST_FOLDER_SOURCE = "select distFolder from sourceconfig where id = ?";

	String INSERT_LOG_DEFAULT = "insert into log values (?,?,?,?)";
	String SET_STATUS_WITH_ID_IN_LOG = "update log set status = ? where id = ?";

}
