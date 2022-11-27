package dao.staging;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import db.DbStagingControlConnection;

public class WeatherFactDao {
	private String procedure;
	private CallableStatement callStmt;
	private PreparedStatement ps;
	private ResultSet rs;
	private Connection connection;

	private LogControllerDao logDao;

	public WeatherFactDao() {
		logDao = new LogControllerDao();
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	public void loadRawFactByLine(int id, String provinceName, String timeLoad, int currentTemp, String overview,
			int lowestTemp, int maximumTemp, float humidity, float vision, float wind, int stopPoint, float uvIndex,
			String airQuality) {
		procedure = Procedure.LOAD_RAW_FACT;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, id);
			callStmt.setString(2, provinceName);
			callStmt.setString(3, timeLoad);
			callStmt.setInt(4, currentTemp);
			callStmt.setString(5, overview);
			callStmt.setInt(6, lowestTemp);
			callStmt.setInt(7, maximumTemp);
			callStmt.setFloat(8, humidity);
			callStmt.setFloat(9, vision);
			callStmt.setFloat(10, wind);
			callStmt.setInt(11, stopPoint);
			callStmt.setFloat(12, uvIndex);
			callStmt.setString(13, airQuality);
			callStmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}

	}

	public boolean transformFact() {
		boolean result = false;
		procedure = Procedure.GET_RAW_FACT;
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			System.out.println(">> Start: transform raw fact");
			while (rs.next()) {
				procedure = Procedure.TRANSFORM_RAW_FACT;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, IdCreater.createIdByCurrentTime());
				callStmt.setString(2, rs.getString("province_name"));
				String[] timestamp = rs.getString("time_load").split(" ");
				callStmt.setString(3, timestamp[0].trim());
				callStmt.setString(4, timestamp[1].trim());
				callStmt.setInt(5, rs.getInt("currentTemp"));
				callStmt.setString(6, rs.getString("overview"));
				callStmt.setInt(7, rs.getInt("lowestTemp"));
				callStmt.setInt(8, rs.getInt("maximumTemp"));
				callStmt.setFloat(9, rs.getFloat("humidity"));
				callStmt.setFloat(10, rs.getFloat("vision"));
				callStmt.setFloat(11, rs.getFloat("wind"));
				callStmt.setInt(12, rs.getInt("stopPoint"));
				callStmt.setFloat(13, rs.getFloat("uvIndex"));
				callStmt.setString(14, rs.getString("airQuality"));
				callStmt.setString(15, rs.getString("time_load"));
				result = callStmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (result) {
			System.out.println(">> End: " + result);
		}
		return result;
	}

	public static void main(String[] args) {
		WeatherFactDao weatherFactDao = new WeatherFactDao();
		weatherFactDao.transformFact();
	}

}
