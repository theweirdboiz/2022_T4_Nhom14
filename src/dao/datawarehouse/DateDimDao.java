package dao.datawarehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import dao.Query;
import db.DbDatawarehouseConnection;

public class DateDimDao {
	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public DateDimDao() {
		connection = DbDatawarehouseConnection.getIntance().getConnect();
	}
	
	public boolean insert(String id, int date, int month, int year
			, int hour, int minute, int second, String dayOfWeek) {
		
		query = Query.INSERT_DATA_TO_DATE_DIM_STAGGING;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			statement.setInt(2, date);
			statement.setInt(3, month);
			statement.setInt(4, year);
			statement.setInt(5, hour);
			statement.setInt(6, minute);
			statement.setInt(7, second);
			statement.setString(8, dayOfWeek);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}
