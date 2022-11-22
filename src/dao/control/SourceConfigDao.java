package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
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
		return id;
	}

	public String getPathFolder(int id) {
		try {
			query = Query.GET_PATH_FOLDER_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setInt(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("path_folder") : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public String getDistFolder(int id) {
		try {
			query = Query.GET_DIST_FOLDER_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setInt(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("dist_folder") : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public String getTimeLoad(int id) {
		try {
			query = Procedure.GET_TIMELOAD;
			ps = connection.prepareStatement(query);
			ps.setInt(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("time_load") : null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
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

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String getPathFolder(String id) {
		try {
			query = Query.GET_PATH_FOLDER_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setString(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("path_folder") : null;
		} catch (SQLException e) {
			return null;
		}
	}
	
	public String getDistFolder(String id) {
		try {
			query = Query.GET_DIST_FOLDER_SOURCE;
			ps = connection.prepareStatement(query);
			ps.setString(1, id);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("dist_folder") : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public static void main(String[] args) throws SQLException {
		SourceConfigDao sourceConfigDao = new SourceConfigDao();
		System.out.println(sourceConfigDao.getTimeLoad(1));
//		System.out.println(sourceConfigDao.getURL("1"));
//		System.out.println(sourceConfigDao.getFileName());
	}
}
