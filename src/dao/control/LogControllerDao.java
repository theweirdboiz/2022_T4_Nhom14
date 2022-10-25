package dao.control;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

import dao.IdCreater;
import dao.Query;
import db.DbControlConnection;

public class LogControllerDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public LogControllerDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public boolean insertLogDefault(String id, String sourceId) {
		try {
			Date date = new Date(Calendar.getInstance().getTime().getTime());
			String status = "ER";
			query = Query.INSERT_LOG_DEFAULT;
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			statement.setString(2, sourceId);
			statement.setDate(3, date);
			statement.setString(4, status);
			return statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean setStatus(String id, String status) {
		try {
			query = Query.SET_STATUS_WITH_ID_IN_LOG;
			statement = connection.prepareStatement(query);
			statement.setString(1, status);
			statement.setString(2, id);
			return statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
}
