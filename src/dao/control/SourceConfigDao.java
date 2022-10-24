package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import db.DbControlConnection;
import db.MySQLConnection;

public class SourceConfigDao {

	String query;
	PreparedStatement ps;
	ResultSet rs;
	Connection connection;

	public SourceConfigDao() {
		connection = DbControlConnection.getIntance().getConnect();

	}

	public String getFileName() throws SQLException {
		String fileName = "";
		query = "SELECT fileName FROM SOURCECONFIG WHERE ID =1";
		ps = connection.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()) {
			fileName = rs.getString("fileName");
		}
		return fileName;
	}

	public String getUrl() throws SQLException {
		String url = "";
		query = "SELECT url FROM SOURCECONFIG WHERE ID =1";
		ps = connection.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()) {
			url = rs.getString("url");
		}
		return url;
	}

	public static void main(String[] args) throws SQLException {
		SourceConfigDao sourceConfigDao = new SourceConfigDao();
		System.out.println(sourceConfigDao.getFileName());
	}
}
