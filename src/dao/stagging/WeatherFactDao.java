package dao.stagging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Query;
import db.DbStaggingConnection;

public class WeatherFactDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public WeatherFactDao() {
		connection = DbStaggingConnection.getIntance().getConnect();
	}
	
	public boolean insert(String dateId, String provinceId, String data) {
		String[] elms = data.split(",");
		try {
			query = Query.INSERT_DATA_TO_WEATHER_FACT_STAGGING;
			statement = connection.prepareStatement(query);
			statement.setString(1, elms[0]);
			statement.setString(2, provinceId);
			statement.setString(3, dateId);
			statement.setInt(4, Integer.parseInt(elms[2].trim()));
			statement.setInt(5, Integer.parseInt(elms[3].trim()));
			statement.setInt(6, Integer.parseInt(elms[4].trim()));
			statement.setFloat(7, Float.parseFloat(elms[5].trim()));
			statement.setString(8, elms[6]);
			statement.setFloat(9, Float.parseFloat(elms[7].trim()));
			statement.setFloat(10, Float.parseFloat(elms[8].trim()));
			statement.setInt(11, Integer.parseInt(elms[9].trim()));
			statement.setFloat(12, Float.parseFloat(elms[10].trim()));
			statement.setString(13, elms[11]);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean checkExistsProvince(String provinceId) {
		
		query = Query.GET_ID_FROM_WEATHER_FACT_BY_PROVINCE;
		
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, provinceId);
			
			ResultSet rs = statement.executeQuery();
			
			return rs.next() ? true : false;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public ResultSet getData() {
		query = Query.GET_DATA_FROM_WEATHERFACT_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public boolean deleteAll(){
		query = Query.DELETE_ALL_WEATHERFACT_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
		WeatherFactDao weatherFactDao = new WeatherFactDao();
		weatherFactDao.insert("7232906a32", "1", "302f741001,Bắc Ninh,30,23,31,0.58,Mây đen u ám,16.78,10.0,22,0.0,Kém");
	}
}
