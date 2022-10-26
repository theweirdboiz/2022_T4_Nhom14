package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;

import com.mysql.cj.protocol.Resultset;

import dao.Query;
import db.DbControlConnection;
import db.MySQLConnection;
import model.DbHosting;
import model.FTPHosting;

public class FTPConfigDao {
//	MySQLConnection connectDb;
//	private final String WEB_URL = "https://thoitiet.vn";
//	private final String DB_URL = "jdbc:mysql://localhost:3306/control";
//	private final String USER_NAME = "root";
//	private final String PASSWORD = "";
//
//	private String FTP_SERVER_ADDRESS = "103.97.126.21";
//	private int FTP_SERVER_PORT_NUMBER = 21;
//	private int FTP_TIMEOUT = 60000;
//	private int BUFFER_SIZE = 1024 * 1024 * 1;
//	private String FTP_USERNAME = "ngsfihae";
//	private String FTP_PASSWORD = "U05IIKw0HsICPNU";
//	private String SLASH = "/";
//	
//	String query;
//	PreparedStatement ps;
//	ResultSet rs;
//
//	public FTPConfigDao(String ip, int port, String userName, String password) {
//		this.FTP_SERVER_ADDRESS = ip;
//		this.FTP_SERVER_PORT_NUMBER = port;
//		this.FTP_USERNAME = userName;
//		this.FTP_PASSWORD = password;
//		connectDb = new MySQLConnection(DB_URL, USER_NAME, PASSWORD);
//	}
//
//	public String getFTP_SERVER_ADDRESS() throws SQLException {
//		query = "SELECT IP FROM FTPConfig WHERE ID=?";
//		ps = connectDb.getConnect().prepareStatement(query);
//		ps.setInt(1, 1);
//		return ps.executeQuery().getString(2);
//	}
//
//	public void setFTP_SERVER_ADDRESS(String fTP_SERVER_ADDRESS) {
//		FTP_SERVER_ADDRESS = fTP_SERVER_ADDRESS;
//	}
//
//	public int getFTP_SERVER_PORT_NUMBER() {
//		return FTP_SERVER_PORT_NUMBER;
//	}
//
//	public void setFTP_SERVER_PORT_NUMBER(int fTP_SERVER_PORT_NUMBER) {
//		FTP_SERVER_PORT_NUMBER = fTP_SERVER_PORT_NUMBER;
//	}
//
//	public String getFTP_USERNAME() {
//		return FTP_USERNAME;
//	}
//
//	public void setFTP_USERNAME(String fTP_USERNAME) {
//		FTP_USERNAME = fTP_USERNAME;
//	}
//
//	public String getFTP_PASSWORD() {
//		return FTP_PASSWORD;
//	}
//
//	public void setFTP_PASSWORD(String fTP_PASSWORD) {
//		FTP_PASSWORD = fTP_PASSWORD;
//	}

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public FTPConfigDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}
	
	public FTPHosting getFTPHosting() {
		try {
			query = Query.GET_FTP_HOSTING;
			statement = connection.prepareStatement(query);
			statement.setInt(1, 1);
			ResultSet result = statement.executeQuery();
			return result.next() 
					? new FTPHosting(result.getString("host"), result.getInt("port"), result.getString("username"), result.getString("password"))
					: null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static void main(String[] args) {
//		FTPHosting ftpHosting = new 
		FTPConfigDao configDao = new FTPConfigDao();
		FTPHosting ftpHosting = configDao.getFTPHosting();
//		System.out.println(ftpHosting);
	}
}
