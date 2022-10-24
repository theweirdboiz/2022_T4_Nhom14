package dao;

public interface Query {
	String GET_DB_HOSTING = "select host, username, password from dbconfig where type = ? and using = ?";
}
