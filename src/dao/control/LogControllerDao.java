package dao.control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import dao.Procedure;
import db.DbControlConnection;

public class LogControllerDao {
	private Connection connection;
	private String procedure;
	private CallableStatement callStmt;
	private ResultSet rs;

	public LogControllerDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	// 1. Lấy một dòng dữ liệu trong log, kiểm tra source này đã được ghi vào ngày
	// hôm nay và giờ hiện tại hay chưa?
	public String getFileStatus(int id) {
		String result = "";
		procedure = Procedure.GET_ONE_FILE_IN_FTP;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, id);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				result = rs.getString("status");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean insertRecord(int logId, int sourceId, String destination) {
		try {
			String status = "ER";

			procedure = Procedure.INSERT_RECORD;

			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			callStmt.setInt(2, sourceId);
			callStmt.setString(3, destination);
			callStmt.setString(4, status);
			return callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public String getDestinationByStatus(int sourceId) {
		try {
			procedure = Procedure.GET_ONE_FILE_IN_FTP;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, sourceId);
			ResultSet rs = callStmt.executeQuery();
			return rs.next() ? rs.getString("destination") : null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void updateStatus(int logId, String status) {
		try {
			procedure = Procedure.UPDATE_STATUS;
			callStmt = connection.prepareCall(procedure);
			callStmt.setString(1, status);
			callStmt.setInt(2, logId);
			callStmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

//Staging
	public int getIdByStatus(int sourceId, String status) {
		int id = 0;
		try {
			procedure = Procedure.GET_ONE_ROW_INFO;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, sourceId);
			callStmt.setString(2, status);
			ResultSet rs = callStmt.executeQuery();
			if (rs.next()) {
				return rs.getInt("id");
			} else {
				System.out.println("vcl");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return id;
	}

	public static void main(String[] args) {
		LogControllerDao logControllerDao = new LogControllerDao();
	}
}
