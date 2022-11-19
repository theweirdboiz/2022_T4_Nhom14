package dao.control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
import db.DbControlConnection;

public class LogControllerDao {
	Connection connection;
	String procedure;
	CallableStatement callableStatement;
	ResultSet rs;

	public LogControllerDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public boolean checkIsExtractedAtCurrentHour(int id) {
		boolean check = false;
		procedure = Procedure.GET_ONE_ROW_FROM_LOG;
		try {
			callableStatement = connection.prepareCall(procedure);
			callableStatement.setInt(1, id);
			rs = callableStatement.executeQuery();
			if (rs.next()) {
				check = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return check;
	}

	public void insertLogDefault(int id, int sourceId) {
		procedure = Procedure.INSERT_RECORD;
		try {
			callableStatement = connection.prepareCall(procedure);
			callableStatement.setInt(1, id);
			callableStatement.setInt(1, sourceId);
			callableStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		LogControllerDao controllerDao = new LogControllerDao();
	}

}
