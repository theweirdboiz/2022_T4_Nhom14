package dao.control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
	public int getLogId() {
		procedure = Procedure.GET_LOG_ID;
		Integer logId = null;
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				logId = rs.getInt("id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return logId;
	}

	public String getDestinationByLogId(int logId) {
		String destination = null;
		procedure = Procedure.GET_DESTINATION_BY_LOG_ID;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				destination = rs.getString("destination");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return destination;
	}

	public int getSourceIdByLogId(int logId) {
		Integer sourceId = null;
		procedure = Procedure.GET_SOURCE_ID_BY_LOG_ID;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				sourceId = rs.getInt("sourceId");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sourceId;
	}

	public String getStatusByLogId(int logId) {
		String status = null;
		procedure = Procedure.GET_STATUS_BY_LOG_ID;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				status = rs.getString("status");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return status;
	}

	public boolean isProvinceDimLoaded() {
		int SOURCE_ID = 3;
		procedure = Procedure.IS_PROVINCE_DIM_EXISTED;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID);
			callStmt.setString(2, "EL");
			rs = callStmt.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

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

	public boolean insertRecord(int logId, int sourceId, String localPath, String ftpPath) {
		try {
			String status = "ER";

			procedure = Procedure.INSERT_RECORD;

			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			callStmt.setInt(2, sourceId);
			callStmt.setString(3, localPath);
			callStmt.setString(4, ftpPath);
			callStmt.setString(5, status);
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

	public ResultSet getLogProvinceDimWithELStatus() {
		procedure = Procedure.GET_ONE_PROVINCEDIM_WITH_EL_STATUS;
		try {
			callStmt = connection.prepareCall(procedure);
			return callStmt.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		LogControllerDao logControllerDao = new LogControllerDao();
		System.out.println(logControllerDao.isProvinceDimLoaded());
//		System.out.println(logControllerDao.getLogId());
//		System.out.println(logControllerDao.getSourceIdByLogId(logControllerDao.getLogId()));
//		System.out.println(logControllerDao.getDestinationByLogId(logControllerDao.getLogId()));
//		System.out.println(logControllerDao.getStatusByLogId(logControllerDao.getLogId()));

	}
}
