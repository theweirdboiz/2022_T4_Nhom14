package dao;

public interface Query {
	String GET_DB_HOSTING = "select host, username, password from dbconfig where type = ? and using = ?";
	String GET_FTP_HOSTING = "select host, port, username, password from ftpconfig where using = ?";
}
