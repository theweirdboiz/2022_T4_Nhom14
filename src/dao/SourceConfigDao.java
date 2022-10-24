package dao;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import db.MySQLConnection;

public class SourceConfigDao {

	String query;
	CallableStatement callStmt;
	PreparedStatement ps;
	ResultSet rs;
	MySQLConnection mySqlConnection;

	public SourceConfigDao() {
		mySqlConnection = new MySQLConnection("jdbc:mysql://localhost:3306/control", "root", "");

	}

	public String getFileName() throws SQLException {
		String fileName = "";
		query = "SELECT fileName FROM SOURCECONFIG WHERE ID =1";
		ps = mySqlConnection.getConnect().prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()) {
			fileName = rs.getString("fileName");
		}
		return fileName;
	}

	public String getUrl() throws SQLException {
		String url = "";
		query = "SELECT url FROM SOURCECONFIG WHERE ID =1";
		ps = mySqlConnection.getConnect().prepareStatement(query);
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
