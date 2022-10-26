package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Query;
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

	public int getId() throws SQLException {
		int id = 1;
		query = "SELECT ID FROM SOURCECONFIG WHERE ID =1";
		ps = connection.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()) {
			id = rs.getInt("ID");
		}
		System.out.println(id);
		return id;
	}

	public String getFileName(int id) throws SQLException {
		String fileName = "";
		query = "SELECT fileName FROM SOURCECONFIG WHERE ID =?";
		ps = connection.prepareStatement(query);
		ps.setInt(1, id);
		rs = ps.executeQuery();
		while (rs.next()) {
			fileName = rs.getString("fileName");
		}
		return fileName;
	}

	public String getUrl(int id) throws SQLException {
		String url = "";
		query = "SELECT url FROM SOURCECONFIG WHERE ID =?";
		ps = connection.prepareStatement(query);
		ps.setInt(1, id);
		rs = ps.executeQuery();
		while (rs.next()) {
			url = rs.getString("url");
		}
		return url;
	}

	public String getURL(String id) {
		try {
			query = Query.GET_URL_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setString(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("url") : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public String getFileName(String id) {
		try {
			query = Query.GET_FILENAME_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setString(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("fileName") : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		SourceConfigDao sourceConfigDao = new SourceConfigDao();
//		System.out.println(sourceConfigDao.getURL("1"));
//		System.out.println(sourceConfigDao.getFileName());
	}
}
