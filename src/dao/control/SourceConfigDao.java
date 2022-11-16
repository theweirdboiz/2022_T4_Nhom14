package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
import dao.SOURCE_ID;
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

	public String getURL() {
		try {
			query = Procedure.GET_URL_SOURCE;
			ps = connection.prepareCall(query);
			ps.setInt(1, SOURCE_ID.getId());
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("url").trim() : null;
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

	public String getPathFolder() {
		try {
			query = Procedure.GET_PATH_FOLDER;
			ps = connection.prepareCall(query);
			ps.setInt(1, SOURCE_ID.getId());
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("pathFolder").trim() : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public String getDistFolder() {
		try {
			query = Procedure.GET_DIST_FOLDER;
			ps = connection.prepareCall(query);
			ps.setInt(1, SOURCE_ID.getId());
			rs = ps.executeQuery();
			return rs.next() ? rs.getString("distFolder").trim() : null;
		} catch (SQLException e) {
			return null;
		}
	}

	public static void main(String[] args) throws SQLException {
		SourceConfigDao sourceConfigDao = new SourceConfigDao();
		System.out.println(sourceConfigDao.getDistFolder());
//		System.out.println(sourceConfigDao.getURL());
//		System.out.println(sourceConfigDao.getFileName());
	}
}
