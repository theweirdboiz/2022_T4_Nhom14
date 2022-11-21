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
	public boolean IsExtracted(int id) {
		boolean result = false;
		procedure = Procedure.GET_ONE_FILE_IN_FTP;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, id);
			rs = callStmt.executeQuery();
			result = rs.next();
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

	public static void main(String[] args) {
		LogControllerDao logControllerDao = new LogControllerDao();
		logControllerDao.insertRecord(0, 0, null);
//		logControllerDao.checkSourceIsExtracted(1);;
	}
}
