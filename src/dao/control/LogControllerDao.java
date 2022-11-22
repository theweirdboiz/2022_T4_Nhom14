package dao.control;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import dao.Query;
import db.DbControlConnection;

public class LogControllerDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public LogControllerDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public boolean insertLogDefault(String id, String sourceId, String path) {
		try {
			Date date = new Date(Calendar.getInstance().getTime().getTime());
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
			String status = "ER";
			query = Query.INSERT_LOG_DEFAULT;
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			statement.setString(2, sourceId);
			statement.setString(3, format.format(date));
			statement.setString(4, path);
			statement.setString(5, status);
			return statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void setStatus(String id, String status) {
		try {
			query = Query.SET_STATUS_WITH_ID_IN_LOG;
			statement = connection.prepareStatement(query);
			statement.setString(1, status);
			statement.setString(2, id);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void setStatusByFTPPath(String ftpPath, String status) {
		try {
			query = Query.SET_STATUS_WITH_FTPPATH_IN_LOG;
			statement = connection.prepareStatement(query);
			statement.setString(1, status);
			statement.setString(2, ftpPath);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public boolean checkExtractedAtHourCurrent(String sourceId) {
		query = Query.CKECK_EXTRACTED_HOUR_CURRENT;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, sourceId);
			
			ResultSet result = statement.executeQuery();
			
			return result.next() ? true : false;
		} catch (SQLException e) {
			return false;
		}
	}
	
	public String getPathFTPWithStatusInLog(String status, String sourceId) {
		query = Query.GET_PATH_SOURCE_WITH_STATE_IN_LOG;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, status);
			statement.setString(2, sourceId);
			
			ResultSet result = statement.executeQuery();
			
			return result.next() ? result.getString("path_ftp") : null; 
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ResultSet getLogHasProvinceDimWithELStatus() {
		query = Query.GET_LOG_WITH_PROVINCE_STATUS_EL;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ResultSet getLogHasWeatherWithELStatus() {
		query = Query.GET_LOG_WITH_WEATHER_STATUS_EL;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void main(String[] args) {
		LogControllerDao controllerDao = new LogControllerDao();
		System.out.println(controllerDao.getPathFTPWithStatusInLog("EO", "3"));
	}
}
