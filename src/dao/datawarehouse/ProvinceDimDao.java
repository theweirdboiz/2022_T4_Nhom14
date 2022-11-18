package dao.datawarehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Query;
import db.DbDatawarehouseConnection;

public class ProvinceDimDao {
	
	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public ProvinceDimDao() {
		connection = DbDatawarehouseConnection.getIntance().getConnect();
	}
	
	public boolean insert(String id, String provinceName) {
		
		query = Query.INSERT_DATA_TO_PROVICE_DATAWAREHOUSE;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			statement.setString(2, provinceName);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public String getIdByProvinceName(String provinceName) {
		query = Query.GET_ID_BY_PROVINCE_NAME_DATAWAREHOUSE;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, provinceName);
			ResultSet resultSet = statement.executeQuery();
			return resultSet.next() ? resultSet.getString("id") : null;
		} catch (SQLException e) {
			return null;
		}
	}
}
