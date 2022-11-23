package dao.staging;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
import db.DbStagingControlConnection;

public class WeatherFactDao {
	private RawWeatherFactDao thoiTietEduVnDao;
	private String procedure;
	private CallableStatement callStmt;
	private PreparedStatement ps;
	private ResultSet rs;
	Connection connection;
	RawWeatherFactDao rawWeatherFactDao;

	public WeatherFactDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
		rawWeatherFactDao = new RawWeatherFactDao();
	}

	public boolean transformFact() {
		boolean result = false;
		if (rawWeatherFactDao.loadRawThoiTietEduVnFact() && rawWeatherFactDao.loadRawThoiTietVnFact()) {
			procedure = "SELECT * FROM raw_weather_data";
			try {
				ps = connection.prepareStatement(procedure);
				rs = ps.executeQuery();
				while (rs.next()) {
					procedure = Procedure.TRANSFORM_WEATHER_FACT;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, rs.getInt("id"));
					callStmt.setString(2, rs.getString("province_name"));
					callStmt.setString(3, rs.getString("date_load"));
					callStmt.setString(4, rs.getString("time_load"));
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
					callStmt.setString(15, rs.getString("date_load") + " " + rs.getString("time_load"));
					result = callStmt.execute();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (result) {
			System.out.println("Transfrom fact successfully!");
		}
		return result;
	}

	public static void main(String[] args) {
		WeatherFactDao weatherFactDao = new WeatherFactDao();
		weatherFactDao.transformFact();
	}

}
