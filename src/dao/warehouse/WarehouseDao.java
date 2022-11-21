package dao.warehouse;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import dao.IdCreater;
import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class WarehouseDao {
	private FTPManager ftpManager;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private Connection connection;
	private DbConfigDao dbConfigDao;
	private DbHosting dbHosting;
	private SourceConfigDao sourceConfigDao;

	String LOAD_DATE_DIM = "{CALL LOAD_DATE_DIM}";
	String LOAD_TIME_DIM = "{CALL LOAD_TIME_DIM}";
	String LOAD_PROVINCE_DIM = "{CALL LOAD_PROVINCE_DIM(?,?,?)}";

	public WarehouseDao() {
		dbConfigDao = new DbConfigDao();
		dbHosting = dbConfigDao.getDatawareHouseHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
	}

	public boolean loadDateDim() {
		boolean result = false;
		procedure = LOAD_DATE_DIM;
		try {
			callStmt = connection.prepareCall(procedure);
			result = callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean loadTimeDim() {
		boolean result = false;
		procedure = LOAD_TIME_DIM;
		try {
			callStmt = connection.prepareCall(procedure);
			result = callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean loadProvinceDim() {
		boolean result = false;
		procedure = LOAD_PROVINCE_DIM;
		String query = "SELECT * FROM staging.provincedim";
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, IdCreater.createIdByCurrentTime());
				callStmt.setInt(2, rs.getInt("id"));
				callStmt.setString(3, rs.getString("name"));
				result = callStmt.executeUpdate() > 0;
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return result;
	}

	public boolean getAllWeatherData() {
		procedure = Procedure.GET_ALL_CURRENT_WEATHER_DATA;
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean insertAllWeatherDataFromStaging() {
		procedure = Procedure.INSERT_ALL_FROM_STAGING;
		boolean result = false;
		try {
			callStmt = connection.prepareCall(procedure);
			result = callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String[] args) {
		WarehouseDao warehouseDao = new WarehouseDao();
//		warehouseDao.getAllWeatherData();
//		System.out.println(warehouseDao.loadTimeDim());
	}

}
