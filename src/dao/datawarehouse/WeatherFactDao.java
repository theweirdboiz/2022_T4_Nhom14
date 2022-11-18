package dao.datawarehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import dao.Query;
import db.DbDatawarehouseConnection;

public class WeatherFactDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public WeatherFactDao() {
		connection = DbDatawarehouseConnection.getIntance().getConnect();
	}
	
	public boolean insert(String sk, String naturalKey, String provinceId, String dateId
			, int currentTemperature, int lowestTemperature, int hightTemperature, float humidity
			, String overview, float wind, float vison, int stopPoint, float uvIndex, String airQuality) {
		
		query = Query.INSERT_DATA_TO_WEATHER_FACT_DATAWAREHOUSE;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, sk);
			statement.setString(2, naturalKey);
			statement.setString(3, provinceId);
			statement.setString(4, dateId);
			statement.setInt(5, currentTemperature);
			statement.setInt(6, lowestTemperature);
			statement.setInt(7, hightTemperature);
			statement.setFloat(8, humidity);
			statement.setString(9, overview);
			statement.setFloat(10, wind);
			statement.setFloat(11, vison);
			statement.setInt(12, stopPoint);
			statement.setFloat(13, uvIndex);
			statement.setString(14, airQuality);
			statement.setInt(15, 0);
			statement.setInt(16, 0);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean setTimeExpried() {
		query = Query.SET_TIMEEXPRIED_IN_WEATHER_FACT;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}
